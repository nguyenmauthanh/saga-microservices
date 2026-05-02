package com.example.saga.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
    List<InventoryReservation> findByOrderId(String orderId);
}
