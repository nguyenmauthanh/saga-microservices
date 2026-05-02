package com.example.saga.order.saga;

import com.example.saga.common.event.OrderEventType;
import com.example.saga.common.event.SagaActions;
import com.example.saga.common.event.SagaReplyActions;
import com.example.saga.order.config.KafkaTopics;
import com.example.saga.order.domain.Order;
import com.example.saga.order.domain.OrderRepository;
import com.example.saga.order.domain.OrderStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Order Saga Orchestrator — 3-step Orchestration Saga
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * REAL-WORLD PATTERN:  E-Commerce Order Fulfillment
 *   Used by: Shopee, Lazada, Amazon, Tokopedia
 *   Problem: Reserve stock, verify the order isn't fraudulent, then charge —
 *            across 3 separate services with 3 separate databases.
 *            Any step can fail; prior steps must be rolled back (compensated).
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Happy path (all 3 steps succeed):
 *   startSaga()
 *     → [1] RESERVE   → inventory-service
 *     ← INVENTORY_RESERVED (success)
 *     → [2] FRAUD_CHECK → fraud-check-service
 *     ← FRAUD_CLEARED
 *     → [3] CHARGE    → payment-service
 *     ← PAYMENT_CHARGED (success)
 *     → publish ORDER_COMPLETED
 *
 * Failure path A — Out of stock (step 1 fails):
 *     ← INVENTORY_RESERVED (failure)
 *     → publish OUT_OF_STOCK   [no compensation: nothing was reserved]
 *
 * Failure path B — Fraud detected (step 2 fails):
 *     ← FRAUD_DECLINED
 *     → [C1] RELEASE → inventory-service
 *     ← INVENTORY_RELEASED
 *     → publish FRAUD_DECLINED
 *
 * Failure path C — Payment declined (step 3 fails):
 *     ← PAYMENT_CHARGED (failure)
 *     → [C1] VOID_FRAUD_CHECK → fraud-check-service (fire-and-forget)
 *     → [C2] RELEASE          → inventory-service
 *     ← INVENTORY_RELEASED
 *     → publish PAYMENT_DECLINED
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Design decisions:
 *   P2  — Idempotency: every reply is deduplicated via idempotency_records.
 *   P7  — Durable orchestrator: all state persisted in saga_instances.
 *   P11 — Partition key = sagaId for ordered delivery within a saga.
 *   VOID_FRAUD_CHECK is fire-and-forget (fraud-service marks it VOIDED but
 *   the orchestrator does not wait for the reply before completing compensation).
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final SagaInstanceRepository sagaRepo;
    private final OrderRepository orderRepo;
    private final IdempotencyRepository idempotency;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    // ── Step 1: Start saga ────────────────────────────────────────────────────

    @Transactional
    public SagaInstance startSaga(Order order) {
        SagaInstance saga = new SagaInstance();
        saga.setId(UUID.randomUUID());
        saga.setOrderId(order.getId().toString());
        saga.setState(SagaState.INVENTORY_RESERVING);
        sagaRepo.save(saga);

        var items = order.getItems().stream()
                .map(i -> Map.of("productId", i.getProductId(), "quantity", i.getQuantity()))
                .toList();

        send(KafkaTopics.INVENTORY_COMMANDS, saga.getId().toString(), Map.of(
                "sagaId",     saga.getId().toString(),
                "orderId",    order.getId().toString(),
                "action",     SagaActions.RESERVE,
                "customerId", order.getCustomerId(),
                "items",      items
        ));

        log.info("[ORCH] Saga {} started → INVENTORY_RESERVING (order={})", saga.getId(), order.getId());
        return saga;
    }

    // ── Reply handler ─────────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaTopics.SAGA_REPLIES, groupId = "order-saga-orchestrator")
    @Transactional
    public void handleReply(String message) throws Exception {
        Map<?, ?> reply  = mapper.readValue(message, Map.class);
        String sagaId    = (String) reply.get("sagaId");
        String action    = (String) reply.get("action");
        boolean success  = Boolean.TRUE.equals(reply.get("success"));

        String iKey = "SAGA_REPLY:" + sagaId + ":" + action;
        if (idempotency.alreadyProcessed(iKey)) {
            log.warn("[ORCH] Duplicate reply {}/{} — skipping", sagaId, action);
            return;
        }

        SagaInstance saga = sagaRepo.findById(UUID.fromString(sagaId)).orElse(null);
        if (saga == null) {
            log.error("[ORCH] No saga found for id {}", sagaId);
            return;
        }
        Order order = orderRepo.findById(UUID.fromString(saga.getOrderId())).orElseThrow();

        log.info("[ORCH] Reply: saga={} action={} success={} state={}",
                sagaId, action, success, saga.getState());

        switch (action) {

            // ── Step 1 reply ──────────────────────────────────────────────────
            case SagaReplyActions.INVENTORY_RESERVED -> {
                if (success) {
                    saga.setState(SagaState.FRAUD_CHECKING);
                    sagaRepo.save(saga);

                    // Step 2: fraud check
                    send(KafkaTopics.FRAUD_CHECK_COMMANDS, sagaId, Map.of(
                            "sagaId",     sagaId,
                            "orderId",    order.getId().toString(),
                            "action",     SagaActions.FRAUD_CHECK,
                            "customerId", order.getCustomerId(),
                            "amount",     order.getTotalAmount().toPlainString()
                    ));
                    log.info("[ORCH] Inventory reserved → FRAUD_CHECKING");
                } else {
                    // Out of stock — nothing was reserved, no compensation needed
                    order.setStatus(OrderStatus.CANCELLED);
                    orderRepo.save(order);
                    saga.setState(SagaState.FAILED);
                    sagaRepo.save(saga);
                    log.warn("[ORCH] Inventory insufficient → FAILED");
                    publishOrderEvent(order, saga, OrderEventType.OUT_OF_STOCK);
                }
            }

            // ── Step 2 reply ──────────────────────────────────────────────────
            case SagaReplyActions.FRAUD_CLEARED -> {
                order.setStatus(OrderStatus.PAYMENT_PROCESSING);
                orderRepo.save(order);
                saga.setState(SagaState.PAYMENT_PROCESSING);
                sagaRepo.save(saga);

                // Step 3: charge payment
                send(KafkaTopics.PAYMENT_COMMANDS, sagaId, Map.of(
                        "sagaId",     sagaId,
                        "orderId",    order.getId().toString(),
                        "action",     SagaActions.CHARGE,
                        "customerId", order.getCustomerId(),
                        "amount",     order.getTotalAmount().toPlainString()
                ));
                log.info("[ORCH] Fraud cleared → PAYMENT_PROCESSING");
            }

            case SagaReplyActions.FRAUD_DECLINED -> {
                // Compensate: release inventory
                saga.setState(SagaState.COMPENSATING);
                sagaRepo.save(saga);
                send(KafkaTopics.INVENTORY_COMMANDS, sagaId, Map.of(
                        "sagaId",  sagaId,
                        "orderId", order.getId().toString(),
                        "action",  SagaActions.RELEASE
                ));
                log.warn("[ORCH] Fraud declined → COMPENSATING (releasing inventory)");
            }

            // ── Step 3 reply ──────────────────────────────────────────────────
            case SagaReplyActions.PAYMENT_CHARGED -> {
                if (success) {
                    order.setStatus(OrderStatus.COMPLETED);
                    orderRepo.save(order);
                    saga.setState(SagaState.COMPLETED);
                    sagaRepo.save(saga);
                    log.info("[ORCH] ✅ Order {} COMPLETED", order.getId());
                    publishOrderEvent(order, saga, OrderEventType.ORDER_COMPLETED);
                } else {
                    // Compensate: void fraud clearance (fire-and-forget) + release inventory
                    saga.setState(SagaState.COMPENSATING_AFTER_PAYMENT);
                    sagaRepo.save(saga);

                    send(KafkaTopics.FRAUD_CHECK_COMMANDS, sagaId, Map.of(
                            "sagaId",  sagaId,
                            "orderId", order.getId().toString(),
                            "action",  SagaActions.VOID_FRAUD_CHECK
                    ));
                    send(KafkaTopics.INVENTORY_COMMANDS, sagaId, Map.of(
                            "sagaId",  sagaId,
                            "orderId", order.getId().toString(),
                            "action",  SagaActions.RELEASE
                    ));
                    log.warn("[ORCH] Payment declined → COMPENSATING_AFTER_PAYMENT");
                }
            }

            // ── Compensation replies ───────────────────────────────────────────
            case SagaReplyActions.INVENTORY_RELEASED -> {
                // Determine outcome BEFORE overwriting the state
                String eventType = saga.getState() == SagaState.COMPENSATING_AFTER_PAYMENT
                        ? OrderEventType.PAYMENT_DECLINED
                        : OrderEventType.FRAUD_DECLINED;

                order.setStatus(OrderStatus.CANCELLED);
                orderRepo.save(order);
                saga.setState(SagaState.FAILED_COMPENSATED);
                sagaRepo.save(saga);

                log.info("[ORCH] ♻️ Saga {} compensated ({}) — order {} cancelled",
                        sagaId, eventType, order.getId());
                publishOrderEvent(order, saga, eventType);
            }

            case SagaReplyActions.FRAUD_VOIDED ->
                log.info("[ORCH] Fraud clearance voided for saga {} (fire-and-forget)", sagaId);

            default -> log.warn("[ORCH] Unknown reply action: {}", action);
        }

        idempotency.markProcessed(iKey);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void send(String topic, String key, Object payload) {
        try {
            kafka.send(topic, key, mapper.writeValueAsString(payload));
        } catch (Exception e) {
            throw new RuntimeException("Failed to send to " + topic, e);
        }
    }

    private void publishOrderEvent(Order order, SagaInstance saga, String eventType) {
        var items = order.getItems().stream()
                .map(i -> Map.of(
                        "productId", i.getProductId(),
                        "quantity",  i.getQuantity(),
                        "unitPrice", i.getUnitPrice().toPlainString()))
                .toList();

        send(KafkaTopics.ORDER_EVENTS, order.getId().toString(), Map.of(
                "eventType",  eventType,
                "orderId",    order.getId().toString(),
                "sagaId",     saga.getId().toString(),
                "customerId", order.getCustomerId(),
                "status",     order.getStatus().name(),
                "sagaState",  saga.getState().name(),
                "total",      order.getTotalAmount().toPlainString(),
                "items",      items
        ));
        log.info("[ORCH] Published {} for order {}", eventType, order.getId());
    }

    public List<SagaInstance> findAll() { return sagaRepo.findAll(); }
}
