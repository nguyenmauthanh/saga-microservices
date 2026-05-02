package com.example.saga.fraud.domain;

import com.example.saga.common.event.SagaActions;
import com.example.saga.common.event.SagaReplyActions;
import com.example.saga.fraud.config.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Fraud Check Service — Kafka command listener.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * REAL-WORLD CONTEXT
 *   In production this service integrates with a risk engine (Stripe Radar,
 *   Kount, Sift). For this demo the rules are simulated:
 *
 *   FRAUD_CHECK rules:
 *     1. customerId contains "fraud"  → DECLINED  (blacklisted customer)
 *     2. amount >= 2000               → DECLINED  (unusually large order)
 *     3. otherwise                    → CLEARED
 *
 *   VOID_FRAUD_CHECK:
 *     Marks a previous CLEARED record as VOIDED when payment later fails.
 *     This is a compensation step — fire-and-forget from the orchestrator.
 *
 * Idempotency: deduplicates redeliveries using the idempotency_records table.
 * Partition key: sagaId ensures ordered delivery within the same saga.
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class FraudCommandListener {

    private final FraudCheckRepository fraudRepo;
    private final IdempotencyStore idempotency;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    @KafkaListener(topics = KafkaTopics.FRAUD_CHECK_COMMANDS, groupId = "fraud-check-service-group")
    @Transactional
    public void handleCommand(String message) throws Exception {
        Map<?, ?> cmd    = mapper.readValue(message, Map.class);
        String action    = (String) cmd.get("action");
        String sagaId    = (String) cmd.get("sagaId");
        String orderId   = (String) cmd.get("orderId");

        log.info("[FRAUD] {} command — saga={} order={}", action, sagaId, orderId);

        switch (action) {
            case SagaActions.FRAUD_CHECK      -> handleFraudCheck(cmd, sagaId, orderId);
            case SagaActions.VOID_FRAUD_CHECK -> handleVoidFraudCheck(sagaId, orderId);
            default                           -> log.warn("[FRAUD] Unknown action: {}", action);
        }
    }

    // ── Fraud check ───────────────────────────────────────────────────────────

    private void handleFraudCheck(Map<?, ?> cmd, String sagaId, String orderId) {
        String iKey = "FRAUD_CHECK:" + orderId;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[FRAUD] Duplicate FRAUD_CHECK for order {} — replying with stored result", orderId);
            FraudCheck existing = fraudRepo.findByOrderId(orderId).orElse(null);
            boolean wasCleared = existing != null && existing.getStatus() == FraudCheckStatus.CLEARED;
            reply(sagaId,
                  wasCleared ? SagaReplyActions.FRAUD_CLEARED : SagaReplyActions.FRAUD_DECLINED,
                  wasCleared,
                  wasCleared ? null : (existing != null ? existing.getDeclineReason() : "Unknown"));
            return;
        }

        String customerId = (String) cmd.get("customerId");
        BigDecimal amount = new BigDecimal(cmd.get("amount").toString());

        // ── Simulated fraud rules ─────────────────────────────────────────────
        String declineReason = null;
        if (customerId != null && customerId.toLowerCase().contains("fraud")) {
            declineReason = "Customer ID flagged as high-risk";
        } else if (amount.compareTo(new BigDecimal("2000")) >= 0) {
            declineReason = "Order amount exceeds risk threshold ($2,000)";
        }

        boolean declined = declineReason != null;

        FraudCheck check = new FraudCheck();
        check.setOrderId(orderId);
        check.setCustomerId(customerId);
        check.setAmount(amount);
        check.setStatus(declined ? FraudCheckStatus.DECLINED : FraudCheckStatus.CLEARED);
        check.setDeclineReason(declineReason);
        fraudRepo.save(check);

        idempotency.markProcessed(iKey);

        if (declined) {
            log.warn("[FRAUD] ⚠️ DECLINED order={} reason={}", orderId, declineReason);
            reply(sagaId, SagaReplyActions.FRAUD_DECLINED, false, declineReason);
        } else {
            log.info("[FRAUD] ✅ CLEARED order={} amount={}", orderId, amount);
            reply(sagaId, SagaReplyActions.FRAUD_CLEARED, true, null);
        }
    }

    // ── Void (compensation) ───────────────────────────────────────────────────

    private void handleVoidFraudCheck(String sagaId, String orderId) {
        fraudRepo.findByOrderId(orderId).ifPresent(check -> {
            if (check.getStatus() == FraudCheckStatus.CLEARED) {
                check.setStatus(FraudCheckStatus.VOIDED);
                fraudRepo.save(check);
                log.info("[FRAUD] ♻️ Voided fraud clearance for order {}", orderId);
            }
        });
        reply(sagaId, SagaReplyActions.FRAUD_VOIDED, true, null);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private void reply(String sagaId, String action, boolean success, String reason) {
        try {
            String payload = mapper.writeValueAsString(Map.of(
                    "sagaId",  sagaId,
                    "action",  action,
                    "success", success,
                    "reason",  reason != null ? reason : ""
            ));
            kafka.send(KafkaTopics.SAGA_REPLIES, sagaId, payload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send reply", e);
        }
    }
}

// ── Idempotency (same pattern as inventory-service and payment-service) ────────

@org.springframework.stereotype.Repository
class IdempotencyStore {

    @jakarta.persistence.PersistenceContext
    private jakarta.persistence.EntityManager em;

    @org.springframework.transaction.annotation.Transactional(
            propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public boolean alreadyProcessed(String key) {
        Long count = em.createQuery(
                "SELECT COUNT(r) FROM IdempotencyRecord r WHERE r.idempotencyKey = :key", Long.class)
                .setParameter("key", key)
                .getSingleResult();
        return count > 0;
    }

    @org.springframework.transaction.annotation.Transactional(
            propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void markProcessed(String key) {
        try {
            em.createNativeQuery(
                    "INSERT INTO idempotency_records (id, idempotency_key) VALUES (gen_random_uuid(), ?)")
                    .setParameter(1, key)
                    .executeUpdate();
        } catch (Exception ignored) { /* UNIQUE constraint = already marked */ }
    }
}

@jakarta.persistence.Entity
@jakarta.persistence.Table(name = "idempotency_records")
class IdempotencyRecord {
    @jakarta.persistence.Id
    @jakarta.persistence.GeneratedValue(strategy = jakarta.persistence.GenerationType.UUID)
    java.util.UUID id;

    @jakarta.persistence.Column(name = "idempotency_key")
    String idempotencyKey;

    @jakarta.persistence.Column(name = "processed_at")
    java.time.LocalDateTime processedAt = java.time.LocalDateTime.now();
}
