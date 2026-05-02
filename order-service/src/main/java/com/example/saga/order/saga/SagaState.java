package com.example.saga.order.saga;

/**
 * State machine for a 3-step Orchestration Saga.
 *
 * <pre>
 * Happy path:
 *   INVENTORY_RESERVING → FRAUD_CHECKING → PAYMENT_PROCESSING → COMPLETED
 *
 * Failure paths:
 *   INVENTORY_RESERVING  → FAILED                (out of stock — nothing to compensate)
 *   FRAUD_CHECKING       → COMPENSATING          → FAILED_COMPENSATED  (fraud declined)
 *   PAYMENT_PROCESSING   → COMPENSATING_AFTER_PAYMENT → FAILED_COMPENSATED  (payment declined)
 * </pre>
 */
public enum SagaState {

    // ── Forward steps ─────────────────────────────────────────────────────────
    /** Waiting for inventory-service to reply to RESERVE command. */
    INVENTORY_RESERVING,
    /** Inventory reserved. Waiting for fraud-check-service to reply to FRAUD_CHECK command. */
    FRAUD_CHECKING,
    /** Fraud cleared. Waiting for payment-service to reply to CHARGE command. */
    PAYMENT_PROCESSING,

    // ── Terminal success ──────────────────────────────────────────────────────
    COMPLETED,

    // ── Terminal failure (no compensation needed) ─────────────────────────────
    /** Inventory was unavailable. No reservations to roll back. */
    FAILED,

    // ── Compensating ─────────────────────────────────────────────────────────
    /**
     * Fraud check failed. Compensating by releasing inventory.
     * Waiting for INVENTORY_RELEASED reply.
     */
    COMPENSATING,

    /**
     * Payment was declined after fraud was cleared.
     * Compensating by voiding fraud clearance AND releasing inventory.
     * Waiting for INVENTORY_RELEASED reply (VOID_FRAUD_CHECK is fire-and-forget).
     */
    COMPENSATING_AFTER_PAYMENT,

    // ── Terminal compensated ──────────────────────────────────────────────────
    FAILED_COMPENSATED
}
