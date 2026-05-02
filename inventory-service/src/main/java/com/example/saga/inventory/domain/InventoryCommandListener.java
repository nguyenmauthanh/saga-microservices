package com.example.saga.inventory.domain;

import com.example.saga.common.event.SagaActions;
import com.example.saga.common.event.SagaReplyActions;
import com.example.saga.inventory.config.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Inventory Service — Kafka command listener.
 *
 * Handles:
 *   RESERVE  → check stock, reserve for order, reply INVENTORY_RESERVED / INVENTORY_FAILED
 *   RELEASE  → release reservation (compensation), reply INVENTORY_RELEASED
 *
 * Idempotency (P2): uses idempotency_records table to deduplicate redeliveries.
 * Partition key (P11): replies use sagaId as key → same partition → ordered.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class InventoryCommandListener {

    private final InventoryItemRepository itemRepo;
    private final InventoryReservationRepository reservationRepo;
    private final IdempotencyStore idempotency;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    @KafkaListener(topics = KafkaTopics.INVENTORY_COMMANDS, groupId = "inventory-service-group")
    @Transactional
    public void handleCommand(String message) throws Exception {
        Map<?, ?> cmd = mapper.readValue(message, Map.class);
        String action  = (String) cmd.get("action");
        String sagaId  = (String) cmd.get("sagaId");
        String orderId = (String) cmd.get("orderId");

        log.info("[INV] {} command — saga={} order={}", action, sagaId, orderId);

        switch (action) {
            case SagaActions.RESERVE -> handleReserve(cmd, sagaId, orderId);
            case SagaActions.RELEASE -> handleRelease(cmd, sagaId, orderId);
            default                  -> log.warn("[INV] Unknown action: {}", action);
        }
    }

    // ── Reserve ───────────────────────────────────────────────────────────────

    private void handleReserve(Map<?, ?> cmd, String sagaId, String orderId) {
        String iKey = "INV_RESERVE:" + orderId;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[INV] Duplicate RESERVE for order {} — replying with actual stored result", orderId);
            boolean wasReserved = reservationRepo.findByOrderId(orderId)
                    .stream().anyMatch(r -> r.getStatus() == ReservationStatus.RESERVED);
            reply(sagaId, SagaReplyActions.INVENTORY_RESERVED, wasReserved,
                    wasReserved ? null : "Reservation previously failed");
            return;
        }

        @SuppressWarnings("unchecked")
        List<Map<?, ?>> items = (List<Map<?, ?>>) cmd.get("items");

        // Check all items have sufficient stock before reserving any
        for (Map<?, ?> item : items) {
            String productId = (String) item.get("productId");
            int qty = ((Number) item.get("quantity")).intValue();

            InventoryItem inv = itemRepo.findByProductId(productId).orElse(null);
            if (inv == null || inv.getAvailableStock() < qty) {
                log.warn("[INV] Insufficient stock for product {} (need {})", productId, qty);
                reply(sagaId, SagaReplyActions.INVENTORY_RESERVED, false,
                        "Insufficient stock for " + productId);
                return;
            }
        }

        // Reserve all items
        for (Map<?, ?> item : items) {
            String productId = (String) item.get("productId");
            int qty = ((Number) item.get("quantity")).intValue();

            InventoryItem inv = itemRepo.findByProductId(productId).orElseThrow();
            inv.setAvailableStock(inv.getAvailableStock() - qty);
            itemRepo.save(inv);

            InventoryReservation res = new InventoryReservation();
            res.setOrderId(orderId);
            res.setProductId(productId);
            res.setQuantity(qty);
            res.setStatus(ReservationStatus.RESERVED);
            reservationRepo.save(res);
        }

        idempotency.markProcessed(iKey);
        log.info("[INV] ✅ Reserved {} item(s) for order {}", items.size(), orderId);
        reply(sagaId, SagaReplyActions.INVENTORY_RESERVED, true, null);
    }

    // ── Release (compensation) ────────────────────────────────────────────────

    private void handleRelease(Map<?, ?> cmd, String sagaId, String orderId) {
        String iKey = "INV_RELEASE:" + orderId;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[INV] Duplicate RELEASE for order {} — idempotent reply", orderId);
            reply(sagaId, SagaReplyActions.INVENTORY_RELEASED, true, null);
            return;
        }

        List<InventoryReservation> reservations = reservationRepo.findByOrderId(orderId);
        for (InventoryReservation res : reservations) {
            if (res.getStatus() == ReservationStatus.RESERVED) {
                InventoryItem inv = itemRepo.findByProductId(res.getProductId()).orElse(null);
                if (inv != null) {
                    inv.setAvailableStock(inv.getAvailableStock() + res.getQuantity());
                    itemRepo.save(inv);
                }
                res.setStatus(ReservationStatus.RELEASED);
                reservationRepo.save(res);
            }
        }

        idempotency.markProcessed(iKey);
        log.info("[INV] ♻️ Released reservations for order {}", orderId);
        reply(sagaId, SagaReplyActions.INVENTORY_RELEASED, true, null);
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
            // P11: sagaId as partition key — ordered within same saga
            kafka.send(KafkaTopics.SAGA_REPLIES, sagaId, payload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send reply", e);
        }
    }
}

// ── Idempotency store (package-private, same package as listener) ─────────────

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
