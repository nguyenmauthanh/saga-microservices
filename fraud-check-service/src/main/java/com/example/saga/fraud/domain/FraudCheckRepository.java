package com.example.saga.fraud.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface FraudCheckRepository extends JpaRepository<FraudCheck, UUID> {
    Optional<FraudCheck> findByOrderId(String orderId);
}
