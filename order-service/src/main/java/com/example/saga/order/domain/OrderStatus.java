package com.example.saga.order.domain;

public enum OrderStatus {
    PENDING,
    INVENTORY_RESERVED,
    PAYMENT_PROCESSING,
    COMPLETED,
    CANCELLED
}
