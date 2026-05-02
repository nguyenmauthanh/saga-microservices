package com.example.saga.inventory.service;

import com.example.saga.inventory.domain.InventoryItemRepository;
import com.example.saga.inventory.domain.InventoryReservationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryItemRepository itemRepo;
    private final InventoryReservationRepository reservationRepo;

    public List<Map<String, Object>> listItems() {
        return itemRepo.findAll().stream()
                .map(i -> Map.<String, Object>of(
                        "productId",      i.getProductId(),
                        "name",           i.getName(),
                        "price",          i.getPrice(),
                        "availableStock", i.getAvailableStock()
                )).toList();
    }

    public List<Map<String, Object>> listReservations() {
        return reservationRepo.findAll().stream()
                .map(r -> Map.<String, Object>of(
                        "orderId",   r.getOrderId(),
                        "productId", r.getProductId(),
                        "quantity",  r.getQuantity(),
                        "status",    r.getStatus()
                )).toList();
    }
}
