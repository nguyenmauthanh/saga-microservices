package com.example.saga.payment.config;

public final class KafkaTopics {
    private KafkaTopics() {}

    public static final String PAYMENT_COMMANDS = "saga.orch.payment-commands";
    public static final String SAGA_REPLIES     = "saga.orch.saga-replies";
}
