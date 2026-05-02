package com.example.saga.order.service;

import com.example.saga.order.domain.Order;
import com.example.saga.order.domain.OrderRepository;
import com.example.saga.order.domain.OrderStatus;
import com.example.saga.order.saga.OrderSagaOrchestrator;
import com.example.saga.order.saga.SagaInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepo;

    @Mock
    private OrderSagaOrchestrator orchestrator;

    @InjectMocks
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        // Simulate JPA UUID generation on save
        doAnswer(inv -> {
            Order o = inv.getArgument(0);
            ReflectionTestUtils.setField(o, "id", UUID.randomUUID());
            return o;
        }).when(orderRepo).save(any(Order.class));

        // Return a fake SagaInstance
        SagaInstance fakeSaga = new SagaInstance();
        ReflectionTestUtils.setField(fakeSaga, "id", UUID.randomUUID());
        when(orchestrator.startSaga(any())).thenReturn(fakeSaga);
    }

    @Test
    void createOrder_calculatesTotal_correctly() {
        var items = List.of(
                new OrderService.OrderItemRequest("product-1", 2, new BigDecimal("10.00")),
                new OrderService.OrderItemRequest("product-2", 3, new BigDecimal("20.00"))
        );

        Map<String, Object> result = orderService.createOrder("customer-1", items, null, null);

        // 2 * 10.00 + 3 * 20.00 = 80.00
        assertThat((BigDecimal) result.get("total")).isEqualByComparingTo("80.00");
    }

    @Test
    void createOrder_usesDefaultPrice_whenUnitPriceIsNull() {
        var items = List.of(
                new OrderService.OrderItemRequest("product-1", 1, null)
        );

        Map<String, Object> result = orderService.createOrder("customer-1", items, null, null);

        // default $49.99 * 1
        assertThat((BigDecimal) result.get("total")).isEqualByComparingTo("49.99");
    }

    @Test
    void createOrder_statusIsPending() {
        var items = List.of(
                new OrderService.OrderItemRequest("product-1", 1, new BigDecimal("100.00"))
        );

        Map<String, Object> result = orderService.createOrder("customer-1", items, null, null);

        assertThat(result.get("status")).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void createOrder_returnsOrderIdSagaIdAndMessage() {
        var items = List.of(
                new OrderService.OrderItemRequest("product-1", 1, new BigDecimal("100.00"))
        );

        Map<String, Object> result = orderService.createOrder("customer-1", items, null, null);

        assertThat(result.get("orderId")).isNotNull();
        assertThat(result.get("sagaId")).isNotNull();
        assertThat(result.get("message")).isNotNull();
    }
}
