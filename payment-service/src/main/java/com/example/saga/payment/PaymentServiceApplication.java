package com.example.saga.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment Service — port 8083
 *
 * Owns: payments
 * Listens to: saga.orch.payment-commands
 * Replies to:  saga.orch.saga-replies
 *
 * Decline rules (simulated):
 *   - customerId contains "declined" → DECLINED
 *   - amount >= $5000               → DECLINED
 */
@SpringBootApplication
public class PaymentServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
