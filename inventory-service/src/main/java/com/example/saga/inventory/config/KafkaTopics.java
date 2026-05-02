package com.example.saga.inventory.config;

public final class KafkaTopics {
    private KafkaTopics() {}

    public static final String INVENTORY_COMMANDS = "saga.orch.inventory-commands";
    public static final String SAGA_REPLIES       = "saga.orch.saga-replies";
}
