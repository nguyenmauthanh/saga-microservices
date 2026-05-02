package com.example.saga.fraud.config;

public final class KafkaTopics {
    private KafkaTopics() {}

    public static final String FRAUD_CHECK_COMMANDS = "saga.orch.fraud-check-commands";
    public static final String SAGA_REPLIES         = "saga.orch.saga-replies";
}
