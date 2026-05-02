package com.example.saga.payment.domain;

import com.example.saga.payment.service.PaymentService;
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
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payments", description = "View payment records")
public class PaymentController {

    private final PaymentService paymentService;

    @Operation(summary = "List all payments")
    @GetMapping
    public ResponseEntity<List<?>> listPayments() {
        return ResponseEntity.ok(paymentService.listPayments());
    }
}
