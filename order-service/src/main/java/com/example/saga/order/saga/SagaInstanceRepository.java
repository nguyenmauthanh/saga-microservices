package com.example.saga.order.saga;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {
    Optional<SagaInstance> findByOrderId(String orderId);
}
