# Handoff Notes

Date: 2026-04-30 22:35 +07:00

## What We Did

- Re-read the Spring Boot project structure from the current source tree.
- Confirmed this is a multi-module Maven microservices project.
- Noted that the project now includes a `common` shared module in addition to the service modules.
- Reviewed the root `pom.xml`, service folders, `application.yml` files, and `docker-compose.yml`.
- Checked the main Saga flow across `order-service`, `inventory-service`, and `payment-service`.
- Checked the gateway routing and authentication filters.
- Checked the customer authentication flow and JWT utility.
- Checked the cart checkout flow and confirmed it now uses Feign clients from `common`.
- Checked downstream event consumers such as BO, notification, shipping, review, promotion, search, and audit.

## Current Understanding

The project uses three main communication styles:

```text
Gateway REST       client entry point
Feign/HTTP         synchronous internal calls during checkout
Kafka events       Saga orchestration and downstream projections
```

The main business flow is:

```text
Customer login/register
  -> JWT issued by customer-service
  -> mobile request enters gateway-service
  -> MobileOnlyFilter checks User-Agent
  -> JwtAuthFilter validates token
  -> request routes to consumer-facing services
```

The main order Saga flow is:

```text
POST /api/orders
  -> order-service creates PENDING order
  -> OrderSagaOrchestrator starts SagaInstance
  -> Kafka RESERVE command to inventory-service
  -> inventory-service reserves stock or fails
  -> Kafka reply to order-service
  -> if inventory reserved, Kafka CHARGE command to payment-service
  -> payment-service charges or declines
  -> order-service completes order or compensates inventory
  -> order-service publishes saga.order.events
```

The cart checkout flow is:

```text
cart-service
  -> validates voucher via promotion-service
  -> calculates shipping fee via shipping-service
  -> creates order via order-service
  -> clears Redis cart on success
```

After final order events, downstream services react:

```text
bo-service           saves admin order projection
notification-service creates email/SMS records
shipping-service     creates shipment for completed orders
review-service       creates verified purchase tokens
promotion-service    confirms or releases voucher usage
audit-service        records event history
```

Catalog, review, and search are also event-driven:

```text
catalog-service publishes product events
  -> search-service updates Elasticsearch

review-service publishes review events
  -> search-service updates rating and review count
```

## Important Files Read

- `pom.xml`
- `docker-compose.yml`
- `common/src/main/java/com/example/saga/common/security/JwtUtil.java`
- `gateway-service/src/main/java/com/example/saga/gateway/filter/MobileOnlyFilter.java`
- `gateway-service/src/main/java/com/example/saga/gateway/filter/JwtAuthFilter.java`
- `customer-service/src/main/java/com/example/saga/customer/controller/AuthController.java`
- `customer-service/src/main/java/com/example/saga/customer/service/AuthService.java`
- `cart-service/src/main/java/com/example/saga/cart/controller/CartController.java`
- `order-service/src/main/java/com/example/saga/order/controller/OrderController.java`
- `order-service/src/main/java/com/example/saga/order/saga/OrderSagaOrchestrator.java`
- `inventory-service/src/main/java/com/example/saga/inventory/domain/InventoryCommandListener.java`
- `payment-service/src/main/java/com/example/saga/payment/domain/PaymentCommandListener.java`
- `bo-service/src/main/java/com/example/saga/bo/filter/BoAuthFilter.java`
- `bo-service/src/main/java/com/example/saga/bo/listener/OrderEventListener.java`
- `notification-service/src/main/java/com/example/saga/notification/listener/OrderEventListener.java`
- `notification-service/src/main/java/com/example/saga/notification/listener/ShipmentEventListener.java`
- `shipping-service/src/main/java/com/example/saga/shipping/listener/OrderEventListener.java`
- `shipping-service/src/main/java/com/example/saga/shipping/service/ShippingService.java`
- `review-service/src/main/java/com/example/saga/review/listener/OrderEventListener.java`
- `review-service/src/main/java/com/example/saga/review/service/ReviewService.java`
- `search-service/src/main/java/com/example/saga/search/listener/ProductEventListener.java`

## Continue From Here

Next time, a useful next step is to verify whether the updated project builds cleanly and whether Docker Compose still matches the current module structure. Pay attention to:

- Whether all services that depend on `common` declare it correctly.
- Whether `cart-service` has Feign enabled in its application class.
- Whether Docker images can build with the `common` module available.
- Whether gateway route overrides include the `auth-service` route consistently.
- Whether `init-databases.sql` includes every database used by the current services.

