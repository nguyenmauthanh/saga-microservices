package com.example.saga.payment.domain;

/** Lifecycle state of a payment record. */
public enum PaymentStatus {
    /** Payment created, awaiting processing. */
    PENDING,
    /** Payment successfully charged. */
    CHARGED,
    /** Payment was declined (insufficient funds, fraud rule, etc.). */
    DECLINED,
    /** Payment has been refunded (saga compensation). */
    REFUNDED
}
