package com.example.saga.order.controller;

import com.example.saga.order.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Tag(name = "Orders", description = "Place orders via Saga Orchestration pattern")
public class OrderController {

    private final OrderService orderService;

    @Operation(
        summary = "Place a new order",
        description = """
            Creates an order and starts the saga orchestration flow:

            1. Order Service creates order (PENDING) and starts saga
            2. Sends RESERVE command → Inventory Service
            3. If reserved: sends CHARGE command → Payment Service
            4. If charged: order → COMPLETED

            **Test customers:**
            - Any customer ID → happy path (amount < $5000)
            - `customer-declined` → payment declined → compensation

            **Test products:**
            - `PRODUCT_001` — $49.99, in stock
            - `PRODUCT_003` — $1299.99, in stock (limited: 3)
            - `PRODUCT_OOS` — out of stock → saga fails immediately
            """
    )
    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(@RequestBody CreateOrderRequest req) {
        return ResponseEntity.ok(orderService.createOrder(
                req.customerId(), req.items(), req.discountAmount(), req.shippingFee()));
    }

    @Operation(summary = "Get order status")
    @GetMapping("/{id}")
    public ResponseEntity<?> getOrder(@PathVariable UUID id) {
        return orderService.findById(id)
                .map(o -> ResponseEntity.ok(Map.of(
                        "id",         o.getId(),
                        "customerId", o.getCustomerId(),
                        "status",     o.getStatus(),
                        "total",      o.getTotalAmount()
                )))
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(summary = "List all orders")
    @GetMapping
    public ResponseEntity<List<?>> listOrders() {
        return ResponseEntity.ok(orderService.findAll().stream()
                .map(o -> Map.of("id", o.getId(), "status", o.getStatus(), "total", o.getTotalAmount()))
                .toList());
    }

    @Operation(summary = "List all saga instances")
    @GetMapping("/sagas")
    public ResponseEntity<List<?>> listSagas() {
        return ResponseEntity.ok(orderService.findAllSagas().stream()
                .map(s -> Map.of("sagaId", s.getId(), "orderId", s.getOrderId(), "state", s.getState()))
                .toList());
    }

    public record CreateOrderRequest(String customerId, List<OrderService.OrderItemRequest> items,
                                     BigDecimal discountAmount, BigDecimal shippingFee) {}
}
