package com.example.saga.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Order Service — port 8081
 *
 * Owns: orders, order_items, saga_instances
 * Runs: saga orchestrator (stateless — all state in DB)
 *
 * Communicates with:
 *   → inventory-service via saga.orch.inventory-commands
 *   → payment-service   via saga.orch.payment-commands
 *   ← both services     via saga.orch.saga-replies
 */
@SpringBootApplication
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
