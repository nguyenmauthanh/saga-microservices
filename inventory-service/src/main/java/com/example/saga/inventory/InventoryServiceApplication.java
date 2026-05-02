package com.example.saga.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Inventory Service — port 8082
 *
 * Owns: inventory_items, inventory_reservations
 * Listens to: saga.orch.inventory-commands
 * Replies to:  saga.orch.saga-replies
 *
 * No HTTP calls to other services — all communication via Kafka.
 */
@SpringBootApplication
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
