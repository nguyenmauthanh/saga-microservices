package com.example.saga.common.event;

/**
 * Reply action values sent back on {@code saga.orch.saga-replies}.
 * The order-service orchestrator switches on these to advance the saga state.
 */
public final class SagaReplyActions {

    // ── Inventory ─────────────────────────────────────────────────────────────
    public static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    public static final String INVENTORY_FAILED   = "INVENTORY_FAILED";
    public static final String INVENTORY_RELEASED = "INVENTORY_RELEASED";

    // ── Fraud Check ───────────────────────────────────────────────────────────
    /** Fraud screening passed — safe to proceed to payment. */
    public static final String FRAUD_CLEARED  = "FRAUD_CLEARED";
    /** Fraud screening failed — order must be cancelled, inventory released. */
    public static final String FRAUD_DECLINED = "FRAUD_DECLINED";
    /** Fraud clearance was voided as part of payment-failure compensation. */
    public static final String FRAUD_VOIDED   = "FRAUD_VOIDED";

    // ── Payment ───────────────────────────────────────────────────────────────
    public static final String PAYMENT_CHARGED  = "PAYMENT_CHARGED";
    public static final String PAYMENT_REFUNDED = "PAYMENT_REFUNDED";

    private SagaReplyActions() {}
}
