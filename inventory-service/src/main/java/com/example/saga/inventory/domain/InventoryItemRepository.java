package com.example.saga.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;

public interface InventoryItemRepository extends JpaRepository<InventoryItem, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<InventoryItem> findByProductId(String productId);
}
