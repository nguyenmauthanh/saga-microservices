package com.example.saga.fraud.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Persisted record of every fraud screening.
 *
 * Real-world analogue: Stripe Radar event, PayPal risk assessment record.
 * Retained for audit — all decisions (CLEARED, DECLINED, VOIDED) are stored.
 */
@Entity
@Table(name = "fraud_checks")
@Getter
@Setter
@NoArgsConstructor
public class FraudCheck {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private FraudCheckStatus status;

    /** The fraud rule that was triggered, or null if CLEARED. */
    @Column(name = "decline_reason")
    private String declineReason;

    @Column(name = "checked_at", nullable = false)
    private LocalDateTime checkedAt = LocalDateTime.now();
}
