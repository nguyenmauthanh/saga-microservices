package com.example.saga.common.event;

/**
 * Command action values sent on the saga command topics.
 *
 * <ul>
 *   <li>{@code saga.orch.inventory-commands}:      RESERVE, RELEASE</li>
 *   <li>{@code saga.orch.fraud-check-commands}:    FRAUD_CHECK, VOID_FRAUD_CHECK</li>
 *   <li>{@code saga.orch.payment-commands}:        CHARGE, REFUND</li>
 * </ul>
 */
public final class SagaActions {

    /** Sent to inventory-service to reserve stock for an order. */
    public static final String RESERVE          = "RESERVE";
    /** Sent to inventory-service to release (compensate) reserved stock. */
    public static final String RELEASE          = "RELEASE";

    /** Sent to fraud-check-service to screen an order before charging. */
    public static final String FRAUD_CHECK      = "FRAUD_CHECK";
    /** Sent to fraud-check-service to void a previous clearance (compensation). */
    public static final String VOID_FRAUD_CHECK = "VOID_FRAUD_CHECK";

    /** Sent to payment-service to charge the customer. */
    public static final String CHARGE           = "CHARGE";
    /** Sent to payment-service to refund a charge (compensation). */
    public static final String REFUND           = "REFUND";

    private SagaActions() {}
}
