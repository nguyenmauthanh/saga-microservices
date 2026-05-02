package com.example.saga.order.service;

import com.example.saga.order.domain.Order;
import com.example.saga.order.domain.OrderItem;
import com.example.saga.order.domain.OrderRepository;
import com.example.saga.order.domain.OrderStatus;
import com.example.saga.order.saga.OrderSagaOrchestrator;
import com.example.saga.order.saga.SagaInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepo;
    private final OrderSagaOrchestrator orchestrator;

    @Transactional
    public Map<String, Object> createOrder(String customerId,
                                           List<OrderItemRequest> items,
                                           BigDecimal discountAmount,
                                           BigDecimal shippingFee) {
        Order order = new Order();
        order.setCustomerId(customerId);
        order.setStatus(OrderStatus.PENDING);

        BigDecimal total = BigDecimal.ZERO;
        for (var item : items) {
            OrderItem oi = new OrderItem();
            oi.setOrder(order);
            oi.setProductId(item.productId());
            oi.setQuantity(item.quantity());
            oi.setUnitPrice(item.unitPrice() != null ? item.unitPrice() : BigDecimal.valueOf(49.99));
            total = total.add(oi.getUnitPrice().multiply(BigDecimal.valueOf(item.quantity())));
            order.getItems().add(oi);
        }
        order.setTotalAmount(total);
        if (discountAmount != null) order.setDiscountAmount(discountAmount);
        if (shippingFee != null)    order.setShippingFee(shippingFee);
        orderRepo.save(order);

        SagaInstance saga = orchestrator.startSaga(order);

        return Map.of(
                "orderId", order.getId(),
                "sagaId",  saga.getId(),
                "status",  order.getStatus(),
                "total",   order.getTotalAmount(),
                "message", "Saga started — poll GET /api/orders/{id} to track status"
        );
    }

    public Optional<Order> findById(UUID id) {
        return orderRepo.findById(id);
    }

    public List<Order> findAll() {
        return orderRepo.findAll();
    }

    public List<SagaInstance> findAllSagas() {
        return orchestrator.findAll();
    }

    public record OrderItemRequest(String productId, int quantity, BigDecimal unitPrice) {}
}
