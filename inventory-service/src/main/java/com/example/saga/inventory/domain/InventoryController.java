package com.example.saga.inventory.domain;

import com.example.saga.inventory.service.InventoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
@Tag(name = "Inventory", description = "View inventory levels and reservations")
public class InventoryController {

    private final InventoryService inventoryService;

    @Operation(summary = "List all inventory items with current stock")
    @GetMapping
    public ResponseEntity<List<?>> listItems() {
        return ResponseEntity.ok(inventoryService.listItems());
    }

    @Operation(summary = "List all reservations")
    @GetMapping("/reservations")
    public ResponseEntity<List<?>> listReservations() {
        return ResponseEntity.ok(inventoryService.listReservations());
    }
}
