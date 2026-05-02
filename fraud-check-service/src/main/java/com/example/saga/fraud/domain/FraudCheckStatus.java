package com.example.saga.fraud.domain;

/** Result of a fraud screening for an order. */
public enum FraudCheckStatus {
    /** Order passed all fraud rules — safe to charge. */
    CLEARED,
    /** Order triggered a fraud rule — do not charge; release inventory. */
    DECLINED,
    /** Clearance was voided because a later saga step (payment) failed. */
    VOIDED
}
