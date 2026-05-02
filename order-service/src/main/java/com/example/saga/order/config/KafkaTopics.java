package com.example.saga.order.config;

/**
 * Kafka topic names shared by convention across services.
 */
public final class KafkaTopics {
    private KafkaTopics() {}

    // Orchestration: Order Service → participants
    public static final String INVENTORY_COMMANDS   = "saga.orch.inventory-commands";
    public static final String FRAUD_CHECK_COMMANDS = "saga.orch.fraud-check-commands";
    public static final String PAYMENT_COMMANDS     = "saga.orch.payment-commands";

    // Orchestration: participants → Order Service (orchestrator)
    public static final String SAGA_REPLIES         = "saga.orch.saga-replies";

    // Domain events: terminal saga outcomes
    public static final String ORDER_EVENTS         = "saga.order.events";
}
