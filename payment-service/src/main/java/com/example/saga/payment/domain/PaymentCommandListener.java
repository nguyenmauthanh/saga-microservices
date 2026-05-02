package com.example.saga.payment.domain;

import com.example.saga.common.event.SagaActions;
import com.example.saga.common.event.SagaReplyActions;
import com.example.saga.payment.config.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Payment Service — Kafka command listener.
 *
 * Handles:
 *   CHARGE → process payment, reply PAYMENT_CHARGED (success/declined)
 *   REFUND → refund payment (compensation), reply PAYMENT_REFUNDED
 *
 * Simulated decline rule: customer ID contains "declined" → payment declined.
 * Simulated decline rule: amount >= 5000 → payment declined.
 *
 * Idempotency (P2): deduplicates redeliveries via idempotency_records table.
 * Partition key (P11): replies keyed by sagaId → ordered delivery.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentCommandListener {

    private final PaymentRepository paymentRepo;
    private final IdempotencyStore idempotency;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    @KafkaListener(topics = KafkaTopics.PAYMENT_COMMANDS, groupId = "payment-service-group")
    @Transactional
    public void handleCommand(String message) throws Exception {
        Map<?, ?> cmd = mapper.readValue(message, Map.class);
        String action     = (String) cmd.get("action");
        String sagaId     = (String) cmd.get("sagaId");
        String orderId    = (String) cmd.get("orderId");
        String customerId = (String) cmd.get("customerId");
        BigDecimal amount = cmd.get("amount") != null
                ? new BigDecimal(cmd.get("amount").toString())
                : BigDecimal.ZERO;

        log.info("[PAY] {} command — saga={} order={} amount={}", action, sagaId, orderId, amount);

        switch (action) {
            case SagaActions.CHARGE -> handleCharge(sagaId, orderId, customerId, amount);
            case SagaActions.REFUND -> handleRefund(sagaId, orderId);
            default                 -> log.warn("[PAY] Unknown action: {}", action);
        }
    }

    // ── Charge ────────────────────────────────────────────────────────────────

    private void handleCharge(String sagaId, String orderId, String customerId, BigDecimal amount) {
        String iKey = "PAY_CHARGE:" + orderId;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[PAY] Duplicate CHARGE for order {} — replying with actual stored result", orderId);
            Payment existing = paymentRepo.findByOrderId(orderId).orElse(null);
            boolean wasCharged = existing != null && existing.getStatus() == PaymentStatus.CHARGED;
            reply(sagaId, SagaReplyActions.PAYMENT_CHARGED, wasCharged, wasCharged ? null : "Payment was declined");
            return;
        }

        // Simulated decline rules
        boolean declined = customerId.toLowerCase().contains("declined") ||
                           amount.compareTo(new BigDecimal("5000")) >= 0;

        Payment payment = new Payment();
        payment.setOrderId(orderId);
        payment.setCustomerId(customerId);
        payment.setAmount(amount);
        payment.setStatus(declined ? PaymentStatus.DECLINED : PaymentStatus.CHARGED);
        paymentRepo.save(payment);

        idempotency.markProcessed(iKey);

        if (declined) {
            log.warn("[PAY] ⚠️ Payment DECLINED for order {} (customer={}, amount={})", orderId, customerId, amount);
            reply(sagaId, SagaReplyActions.PAYMENT_CHARGED, false, "Payment declined");
        } else {
            log.info("[PAY] ✅ Charged ${} for order {}", amount, orderId);
            reply(sagaId, SagaReplyActions.PAYMENT_CHARGED, true, null);
        }
    }

    // ── Refund (compensation) ─────────────────────────────────────────────────

    private void handleRefund(String sagaId, String orderId) {
        String iKey = "PAY_REFUND:" + orderId;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[PAY] Duplicate REFUND for order {} — idempotent reply", orderId);
            reply(sagaId, SagaReplyActions.PAYMENT_REFUNDED, true, null);
            return;
        }

        paymentRepo.findByOrderId(orderId).ifPresent(p -> {
            p.setStatus(PaymentStatus.REFUNDED);
            paymentRepo.save(p);
        });

        idempotency.markProcessed(iKey);
        log.info("[PAY] ♻️ Refunded order {}", orderId);
        reply(sagaId, SagaReplyActions.PAYMENT_REFUNDED, true, null);
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

// ── Idempotency infrastructure ────────────────────────────────────────────────

@org.springframework.stereotype.Repository
class IdempotencyStore {

    @PersistenceContext
    private EntityManager em;

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

@Entity
@Table(name = "idempotency_records")
class IdempotencyRecord {
    @Id @GeneratedValue(strategy = GenerationType.UUID) UUID id;
    @Column(name = "idempotency_key") String idempotencyKey;
    @Column(name = "processed_at") LocalDateTime processedAt = LocalDateTime.now();
}
