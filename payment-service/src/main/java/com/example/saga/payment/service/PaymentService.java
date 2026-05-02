package com.example.saga.payment.service;

import com.example.saga.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepo;

    public List<Map<String, Object>> listPayments() {
        return paymentRepo.findAll().stream()
                .map(p -> Map.<String, Object>of(
                        "id",         p.getId(),
                        "orderId",    p.getOrderId(),
                        "customerId", p.getCustomerId(),
                        "amount",     p.getAmount(),
                        "status",     p.getStatus()
                )).toList();
    }
}
