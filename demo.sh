#!/usr/bin/env bash
# =============================================================================
# Saga Pattern Demo — 3-Step E-Commerce Order Fulfillment
#
# Real-world use case: Used by Shopee, Lazada, Amazon, Tokopedia.
# Problem: Reserve stock, check for fraud, charge payment — across 3 separate
#          services with 3 separate databases. Any step can fail; prior steps
#          must be rolled back (compensated).
#
# Prerequisite:
#   mvn package -DskipTests
#   docker compose up --build -d
#   Wait ~60s for all services to be healthy (docker compose ps)
#
# Usage: bash demo.sh
# =============================================================================

BASE="http://localhost:8081/api/orders"
SEP="─────────────────────────────────────────────────────────────"

wait_and_check() {
  local orderId=$1
  echo "  Waiting 3s for saga to complete..."
  sleep 3
  echo "  Order state:"
  curl -s "http://localhost:8081/api/orders/${orderId}" | \
    grep -oE '"status":"[^"]*"|"sagaState":"[^"]*"' | sed 's/^/    /'
  echo ""
}

echo ""
echo "  3-Step Orchestration Saga: INVENTORY -> FRAUD CHECK -> PAYMENT"
echo ""
echo "$SEP"
echo ""

# =============================================================================
# Scenario A: Happy Path — all 3 steps succeed
# =============================================================================
echo "SCENARIO A — Happy Path"
echo "  Flow: INVENTORY_RESERVED -> FRAUD_CLEARED -> PAYMENT_CHARGED"
echo "  Expected outcome: ORDER_COMPLETED"
echo ""

RESP=$(curl -s -X POST "$BASE" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-1",
    "items": [{"productId": "prod-laptop", "quantity": 1, "unitPrice": 999.99}]
  }')
echo "  Request:  customerId=customer-1, prod-laptop x1, amount=999.99"
echo "  Response: $RESP"
ORDER_ID=$(echo "$RESP" | grep -oE '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_and_check "$ORDER_ID"
echo "$SEP"

# =============================================================================
# Scenario B: Out of Stock — step 1 fails, no compensation needed
# =============================================================================
echo "SCENARIO B — Out of Stock"
echo "  Flow: INVENTORY_RESERVED(false)"
echo "  No compensation needed — nothing was reserved"
echo "  Expected outcome: FAILED  |  Event: OUT_OF_STOCK"
echo ""

RESP=$(curl -s -X POST "$BASE" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-1",
    "items": [{"productId": "prod-nonexistent", "quantity": 999, "unitPrice": 10.00}]
  }')
echo "  Request:  prod-nonexistent qty=999 (no stock)"
echo "  Response: $RESP"
ORDER_ID=$(echo "$RESP" | grep -oE '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_and_check "$ORDER_ID"
echo "$SEP"

# =============================================================================
# Scenario C: Fraud Detected — step 2 fails, compensate step 1
# Fraud rule: customerId contains "fraud"
# =============================================================================
echo "SCENARIO C — Fraud Detected (blacklisted customer)"
echo "  Flow: INVENTORY_RESERVED -> FRAUD_DECLINED -> [C] INVENTORY_RELEASED"
echo "  Compensation: inventory reservation rolled back"
echo "  Expected outcome: FAILED_COMPENSATED  |  Event: FRAUD_DECLINED"
echo ""

RESP=$(curl -s -X POST "$BASE" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-fraud-suspect",
    "items": [{"productId": "prod-laptop", "quantity": 1, "unitPrice": 999.99}]
  }')
echo "  Request:  customerId=customer-fraud-suspect (triggers fraud rule: name contains 'fraud')"
echo "  Response: $RESP"
ORDER_ID=$(echo "$RESP" | grep -oE '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_and_check "$ORDER_ID"
echo "$SEP"

# =============================================================================
# Scenario C2: High-Value Order Flagged — step 2 fails, compensate step 1
# Fraud rule: amount >= 2000
# =============================================================================
echo "SCENARIO C2 — High-Value Order Flagged"
echo "  Flow: INVENTORY_RESERVED -> FRAUD_DECLINED -> [C] INVENTORY_RELEASED"
echo "  Fraud rule: amount >= 2000 triggers risk threshold"
echo "  Expected outcome: FAILED_COMPENSATED  |  Event: FRAUD_DECLINED"
echo ""

RESP=$(curl -s -X POST "$BASE" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-1",
    "items": [{"productId": "prod-laptop", "quantity": 1, "unitPrice": 2500.00}]
  }')
echo "  Request:  customerId=customer-1, amount=2500.00 (triggers risk threshold)"
echo "  Response: $RESP"
ORDER_ID=$(echo "$RESP" | grep -oE '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_and_check "$ORDER_ID"
echo "$SEP"

# =============================================================================
# Scenario D: Payment Declined — step 3 fails, compensate steps 1+2
# Payment rule: customerId contains "declined"
# =============================================================================
echo "SCENARIO D — Payment Declined"
echo "  Flow: INVENTORY_RESERVED -> FRAUD_CLEARED -> PAYMENT_CHARGED(false)"
echo "  Compensation: VOID_FRAUD_CHECK (fire-and-forget) + INVENTORY_RELEASED"
echo "  Expected outcome: FAILED_COMPENSATED  |  Event: PAYMENT_DECLINED"
echo ""

RESP=$(curl -s -X POST "$BASE" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-declined",
    "items": [{"productId": "prod-laptop", "quantity": 1, "unitPrice": 999.99}]
  }')
echo "  Request:  customerId=customer-declined (triggers payment decline rule)"
echo "  Response: $RESP"
ORDER_ID=$(echo "$RESP" | grep -oE '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_and_check "$ORDER_ID"
echo "$SEP"

echo "Demo complete."
echo ""
echo "Watch the full Kafka message flow at: http://localhost:8090"
echo ""
echo "  saga.orch.inventory-commands   — RESERVE / RELEASE"
echo "  saga.orch.fraud-check-commands — FRAUD_CHECK / VOID_FRAUD_CHECK"
echo "  saga.orch.payment-commands     — CHARGE"
echo "  saga.orch.saga-replies         — all participant replies"
echo "  saga.order.events              — terminal outcomes"
echo ""
