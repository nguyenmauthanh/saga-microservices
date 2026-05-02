package com.example.saga.common.event;

/**
 * Terminal outcome event types published to {@code saga.order.events}.
 *
 * <pre>
 *   ORDER_COMPLETED  — all saga steps succeeded
 *   OUT_OF_STOCK     — inventory step failed; no charge was made
 *   FRAUD_DECLINED   — fraud check failed; inventory was released
 *   PAYMENT_DECLINED — payment step failed; fraud cleared + inventory released
 * </pre>
 */
public final class OrderEventType {

    public static final String ORDER_COMPLETED  = "ORDER_COMPLETED";
    public static final String OUT_OF_STOCK     = "OUT_OF_STOCK";
    public static final String FRAUD_DECLINED   = "FRAUD_DECLINED";
    public static final String PAYMENT_DECLINED = "PAYMENT_DECLINED";

    private OrderEventType() {}
}
