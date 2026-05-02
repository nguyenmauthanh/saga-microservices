# Saga Pattern — Microservices Demo

Three real Spring Boot microservices communicating **only via Kafka**.
No HTTP calls between services. Each service owns a separate database.
The entire stack runs with two commands.

---

## Table of Contents

1. [What This Project Demonstrates](#what-this-project-demonstrates)
2. [Architecture Overview](#architecture-overview)
3. [How to Run](#how-to-run)
4. [Demo — 4 Scenarios](#demo--4-scenarios)
5. [Recording a Demo GIF](#recording-a-demo-gif)
6. [Observing with Kafka UI](#observing-with-kafka-ui)
7. [How Docker Compose Works Here](#how-docker-compose-works-here)
8. [How CDC (Change Data Capture) Works](#how-cdc-change-data-capture-works)
9. [Key Patterns Implemented](#key-patterns-implemented)
10. [Troubleshooting](#troubleshooting)
11. [Project Structure](#project-structure)

---

## What This Project Demonstrates

| Concept | Where |
|---|---|
| **Orchestration Saga** — central state machine drives steps | `OrderSagaOrchestrator.java` |
| **Compensating transaction** — undo a step when a later step fails | `RELEASE` inventory command on payment decline |
| **Durable orchestrator state** — crash-safe, no in-memory state | `saga_instances` DB table |
| **Idempotency** — safe to replay the same Kafka message twice | `idempotency_records` table in each service |
| **Partition key** — all saga messages go to the same Kafka partition | `sagaId` used as Kafka message key |
| **Database-per-service** — each service owns its own schema | `order_db`, `inventory_db`, `payment_db` |
| **Event-driven communication** — zero HTTP calls between services | Kafka topics only |

---

## Architecture Overview

```
  Browser / curl
       │
       │  POST /api/orders
       ▼
 ┌─────────────────────────────────────┐
 │          Order Service  :8081        │
 │                                     │
 │  OrderController                    │
 │  OrderSagaOrchestrator ←── replies  │
 │  DB: order_db                       │
 │    orders, order_items              │
 │    saga_instances                   │
 │    idempotency_records              │
 └──────────────┬──────────────────────┘
                │
       ┌────────┴─────────┐
       │ Kafka            │
       │                  │
       ▼                  ▼
 ┌───────────────┐  ┌─────────────────┐
 │Inventory Svc  │  │  Payment Svc    │
 │  :8082        │  │  :8083          │
 │               │  │                 │
 │ DB:           │  │ DB:             │
 │ inventory_db  │  │ payment_db      │
 │  items        │  │  payments       │
 │  reservations │  │  idempotency    │
 │  idempotency  │  │                 │
 └───────┬───────┘  └────────┬────────┘
         │   [replies]       │
         └────────┬──────────┘
                  ▼
         saga.orch.saga-replies
         (consumed by Order Service)
```

### Kafka Topics

| Topic | From → To | Payload |
|---|---|---|
| `saga.orch.inventory-commands` | Order → Inventory | `{action: RESERVE/RELEASE, orderId, items[]}` |
| `saga.orch.payment-commands` | Order → Payment | `{action: CHARGE, orderId, customerId, amount}` |
| `saga.orch.saga-replies` | Inventory/Payment → Order | `{sagaId, action, success, reason}` |

All messages are **plain JSON strings**. No Avro, no schema registry.
The `sagaId` is always the **Kafka message key** — this guarantees all messages for one saga land on the same partition and are consumed in order.

### Databases

| Service | DB | Tables |
|---|---|---|
| Order Service | `order_db` | `orders`, `order_items`, `saga_instances`, `idempotency_records` |
| Inventory Service | `inventory_db` | `inventory_items`, `inventory_reservations`, `idempotency_records` |
| Payment Service | `payment_db` | `payments`, `idempotency_records` |

### Seed Data (Inventory)

| productId | Name | Price | Stock |
|---|---|---|---|
| `PRODUCT_001` | Wireless Headphones | $49.99 | 50 |
| `PRODUCT_002` | Mechanical Keyboard | $199.99 | 100 |
| `PRODUCT_003` | Ultra-wide Monitor | $1,299.99 | 3 |
| `PRODUCT_EXP` | Gold MacBook Pro | $5,999.99 | 5 |
| `PRODUCT_OOS` | Sold-Out Widget | $9.99 | **0** |

### Payment Decline Rules (simulated)

| Condition | Result |
|---|---|
| `customerId` contains `"declined"` | Payment DECLINED |
| `amount >= $5,000` | Payment DECLINED |
| Everything else | Payment CHARGED |

---

## How to Run

### Prerequisites

- **Docker Desktop** — running and healthy
- **Java 21** + **Maven 3.8+** — for building JARs locally before packaging into Docker

> Why build locally? Docker's BuildKit runs in its own network namespace and can
> have DNS issues pulling images (e.g. `maven:3.8.8`) on some Windows/VPN setups.
> Building with local Maven is reliable and faster for development.

### Step 1 — Build the JARs

```bash
cd saga-microservices
mvn package -DskipTests
```

This compiles all 3 services and produces:
- `order-service/target/order-service-1.0.0.jar`
- `inventory-service/target/inventory-service-1.0.0.jar`
- `payment-service/target/payment-service-1.0.0.jar`

### Step 2 — Start everything

```bash
docker compose up --build -d
```

`--build` packages each JAR into a slim Docker image (copies JAR → `eclipse-temurin:21-jre-jammy`).
The infrastructure containers (Kafka, PostgreSQL) start first due to `depends_on: condition: service_healthy`.

**First run**: ~2 min (downloads base images).
**Subsequent runs**: ~15 seconds (images cached).

### Step 3 — Wait for services to be healthy

```bash
docker compose ps
```

Wait until all 7 containers show `healthy` or `Up`:

```
NAME                                   STATUS
saga-microservices-zookeeper-1         Up (healthy)
saga-microservices-kafka-1             Up (healthy)
saga-microservices-postgres-1          Up (healthy)
saga-microservices-kafka-ui-1          Up
saga-microservices-order-service-1     Up (healthy)
saga-microservices-inventory-service-1 Up (healthy)
saga-microservices-payment-service-1   Up (healthy)
```

Or watch the logs:

```bash
docker compose logs -f order-service inventory-service payment-service
# Wait for: "Started OrderServiceApplication in X.X seconds"
```

### Step 4 — Open the UIs

| URL | Description |
|---|---|
| http://localhost:8081/swagger-ui.html | Order Service — place orders here |
| http://localhost:8082/swagger-ui.html | Inventory Service — view stock |
| http://localhost:8083/swagger-ui.html | Payment Service — view payments |
| http://localhost:8090 | **Kafka UI** — watch messages flow in real time |

### Workflow after code changes

```bash
# 1. Rebuild only the changed service JAR
mvn package -DskipTests -pl order-service

# 2. Rebuild its Docker image and restart it (other services keep running)
docker compose up --build -d order-service
```

### Stop everything

```bash
docker compose down        # stop + keep DB data
docker compose down -v     # stop + wipe DB (fresh start)
```

---

## Demo — 4 Scenarios

Run all 4 scenarios automatically:

```bash
bash demo.sh
```

Or run them manually:

---

### Scenario 1 — Happy Path

```
Order → [RESERVE] → Inventory reserves stock
      → [CHARGE]  → Payment charges customer
      → Order COMPLETED, Saga COMPLETED
```

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-1",
    "items": [{"productId": "PRODUCT_001", "quantity": 2, "unitPrice": 49.99}]
  }'

# Response:
# { "orderId": "...", "sagaId": "...", "status": "PENDING", "total": 99.98 }

# Poll after 3 seconds:
curl http://localhost:8081/api/orders/{orderId}
# { "status": "COMPLETED", ... }

curl http://localhost:8081/api/orders/sagas
# [{ "state": "COMPLETED", ... }]
```

**What happens step by step:**

```
Time  Event
 0ms  POST /api/orders received
      → order saved (status=PENDING)
      → saga saved (state=INVENTORY_RESERVING)
      → RESERVE command sent to kafka:29092 → saga.orch.inventory-commands

~50ms Inventory Service receives RESERVE command
      → checks stock: PRODUCT_001 has 50, need 2 ✓
      → decrements stock: 50 → 48
      → saves reservation (status=RESERVED)
      → reply sent → saga.orch.saga-replies {success: true}

~100ms Order Service receives reply
       → saga transitions INVENTORY_RESERVING → PAYMENT_PROCESSING
       → CHARGE command sent → saga.orch.payment-commands

~150ms Payment Service receives CHARGE command
       → customer-1 is not declined, amount $99.98 < $5000 ✓
       → saves payment (status=CHARGED)
       → reply sent → saga.orch.saga-replies {success: true}

~200ms Order Service receives reply
       → order status: PENDING → COMPLETED
       → saga state: PAYMENT_PROCESSING → COMPLETED
```

---

### Scenario 2 — Inventory Fails (Out of Stock)

```
Order → [RESERVE] → Inventory: "no stock" → reply false
      → Order CANCELLED, Saga FAILED
      (no payment attempted, no compensation needed)
```

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-1",
    "items": [{"productId": "PRODUCT_OOS", "quantity": 1, "unitPrice": 9.99}]
  }'

# After 3s:
curl http://localhost:8081/api/orders/{orderId}
# { "status": "CANCELLED" }
# saga state = FAILED
```

---

### Scenario 3 — Payment Declined → Compensation

```
Order → [RESERVE] → Inventory reserves stock ✓
      → [CHARGE]  → Payment: "customer-declined" → reply false
      → [RELEASE] → Inventory releases stock back ✓
      → Order CANCELLED, Saga FAILED_COMPENSATED
```

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-declined",
    "items": [{"productId": "PRODUCT_001", "quantity": 3, "unitPrice": 49.99}]
  }'

# After 4s:
curl http://localhost:8081/api/orders/{orderId}
# { "status": "CANCELLED" }
# saga state = FAILED_COMPENSATED

# Verify stock was restored:
curl http://localhost:8082/api/inventory
# PRODUCT_001 availableStock should be back to where it started
```

**What the compensation looks like:**

```
Inventory reserved:  stock 48 → 45  (decremented 3)
Payment declined:    status  = DECLINED
RELEASE command:     stock 45 → 48  (restored 3)  ← compensation
```

---

### Scenario 4 — High-Value Declined ($5,000 rule)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-vip",
    "items": [{"productId": "PRODUCT_EXP", "quantity": 1, "unitPrice": 5999.99}]
  }'

# After 4s: status=CANCELLED, saga=FAILED_COMPENSATED
# (amount $5999.99 >= $5000 → auto-declined regardless of customerId)
```

---

### Check all state after running scenarios

```bash
# All saga states
curl http://localhost:8081/api/orders/sagas

# All payments
curl http://localhost:8083/api/payments

# Inventory levels (watch PRODUCT_001 stock change across scenarios)
curl http://localhost:8082/api/inventory
```

---

## Recording a Demo GIF

A GIF makes it easy to share how the project works. Here's how to record one on Windows.

### Option A — ScreenToGif (recommended for Windows)

1. Download **ScreenToGif** from https://www.screentogif.com
2. Open a terminal and run `bash demo.sh`
3. In ScreenToGif, choose **Recorder → Screen** and position the capture area over the terminal
4. Press **F7** to start recording, run the demo, press **F8** to stop
5. In the editor, trim silent parts, set frame delay to ~80ms, export as GIF

### Option B — PowerShell + asciinema (text-based, smaller file)

```powershell
# Install asciinema
pip install asciinema

# Record
asciinema rec demo.cast

# Run the demo inside the recording session
bash demo.sh

# Convert to GIF
pip install agg
agg demo.cast demo.gif
```

### What to show in the GIF

A good 30-second GIF covers:

```
1. docker compose ps              → show all 7 containers healthy
2. bash demo.sh (Scenario 1)      → show COMPLETED in terminal
3. bash demo.sh (Scenario 3)      → show FAILED_COMPENSATED + stock restored
4. Open Kafka UI http://localhost:8090 → show messages in topics
```

---

## Observing with Kafka UI

Open **http://localhost:8090** and explore:

### Topics tab

You should see these topics auto-created after running at least one order:

| Topic | What you'll find there |
|---|---|
| `saga.orch.inventory-commands` | RESERVE / RELEASE commands sent by Order Service |
| `saga.orch.payment-commands` | CHARGE commands sent by Order Service |
| `saga.orch.saga-replies` | Replies from Inventory + Payment back to Orchestrator |

### Inspecting a message

Click a topic → **Messages** → click any message. You'll see:

**RESERVE command** (from Order Service to Inventory):
```json
{
  "sagaId":  "8a759ca5-...",
  "orderId": "133dc305-...",
  "action":  "RESERVE",
  "customerId": "customer-1",
  "amount": "49.99",
  "items": [
    { "productId": "PRODUCT_001", "quantity": 2 }
  ]
}
```

**Reply** (from Inventory Service back to Order):
```json
{
  "sagaId":  "8a759ca5-...",
  "action":  "INVENTORY_RESERVED",
  "success": true,
  "reason":  ""
}
```

**Partition key**: Every message has a key equal to `sagaId`. Click the key column — all 3 messages for the same saga will have the same key, meaning they land on the **same partition** and are consumed in order.

### Consumer Groups tab

You can verify each service is consuming from the right topic:

| Consumer Group | Topic | Lag |
|---|---|---|
| `order-saga-orchestrator` | `saga.orch.saga-replies` | 0 (caught up) |
| `inventory-service-group` | `saga.orch.inventory-commands` | 0 |
| `payment-service-group` | `saga.orch.payment-commands` | 0 |

If lag > 0, a service is behind — check its logs with `docker compose logs -f inventory-service`.

---

## How Docker Compose Works Here

### Startup dependency chain

`docker compose up -d` starts containers in a specific order enforced by `depends_on + condition: service_healthy`:

```
Step 1:  zookeeper starts
         healthcheck: echo ruok | nc localhost 2181
         → Kafka needs Zookeeper to register itself

Step 2:  kafka starts  (waits for zookeeper healthy)
         healthcheck: kafka-broker-api-versions --bootstrap-server localhost:9092
         → takes ~15s on first run

Step 3:  postgres starts  (in parallel with kafka)
         healthcheck: pg_isready -U saga
         → init-databases.sql runs on first start (creates 3 databases)

Step 4:  order-service, inventory-service, payment-service, kafka-ui
         (all start in parallel, only after kafka + postgres are healthy)
         healthcheck: curl http://localhost:{port}/actuator/health
         → Spring Boot + Flyway migrations run, Kafka listeners connect
```

### Two Kafka listeners — why?

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
```

Kafka tells clients: "to connect to me, use this address". The problem is that inside Docker, `localhost` refers to the container itself — not the Kafka container. So:

```
Your host machine uses:     localhost:9092    (mapped by Docker port binding)
Spring containers use:      kafka:29092       (Docker internal hostname)
```

If you only advertised `localhost:9092`, the Spring services would try connecting to `localhost` inside their own container and fail.

### How environment variables override application.yml

The `application.yml` files contain `localhost` URLs (for running locally with `mvn spring-boot:run`):

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/order_db
  kafka:
    bootstrap-servers: localhost:9092
```

Docker Compose injects environment variables that Spring Boot automatically picks up:

```yaml
environment:
  SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/order_db
  SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

Spring Boot's property binding follows this priority:
```
Environment variables > application.yml > defaults
```

So the same JAR works both locally (`localhost`) and in Docker (`postgres`, `kafka`) without any code changes. Just different environment variables.

### How the Dockerfile works (current setup)

```dockerfile
# Uses a pre-built JAR (built by local Maven: mvn package -DskipTests)
FROM eclipse-temurin:21-jre-jammy     # ~200MB slim Ubuntu + JRE

# Install curl (needed by Docker healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends curl

WORKDIR /app
COPY target/*.jar app.jar             # copy the fat JAR from local target/

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

The final image contains only: Ubuntu base + JRE + your application JAR.
No source code, no Maven, no JDK — keeps images lean.

### Flyway migrations run automatically

Each service's Flyway runs on startup and applies migrations in version order:

```
Order Service:
  V1__init.sql  → creates orders, order_items, saga_instances, idempotency_records

Inventory Service:
  V1__init.sql  → creates inventory_items, inventory_reservations, idempotency_records
  V2__seed.sql  → inserts PRODUCT_001 through PRODUCT_OOS

Payment Service:
  V1__init.sql  → creates payments, idempotency_records
```

If you run `docker compose down -v` and restart, the migrations re-run from scratch and inventory is re-seeded.

---

## How CDC (Change Data Capture) Works

This project publishes events by writing commands directly to Kafka via `KafkaTemplate`. This is simple and works, but has a hidden fragility called the **dual write problem**. CDC is the production solution.

### The dual write problem

Every saga step does two things:

```
1. Write to database (e.g., update inventory stock)
2. Publish to Kafka (e.g., send INVENTORY_RESERVED reply)
```

These are **two different systems**. They cannot be wrapped in a single ACID transaction. If the app crashes between step 1 and step 2:

```java
inventoryRepo.save(reservation);   // ✅ DB committed
kafkaTemplate.send(reply);         // 💥 app crashes here
                                   // Kafka never received the message
                                   // Orchestrator waits forever
                                   // Saga stuck in INVENTORY_RESERVING
```

### Fix: Transactional Outbox

Instead of publishing directly to Kafka, write an **outbox record** in the same DB transaction:

```java
@Transactional
public void handleReserve(...) {
    // Business write
    inventoryRepo.save(reservation);

    // Outbox write — SAME transaction, SAME commit
    outboxRepo.save(new OutboxEvent(
        topic  = "saga.orch.saga-replies",
        key    = sagaId,                       // partition key
        payload = "{success: true, ...}"
    ));
    // If anything fails here, BOTH writes roll back atomically
}
```

A separate **relay** process publishes outbox rows to Kafka:

```java
@Scheduled(fixedDelay = 1000)
void relay() {
    outboxRepo.findByStatus(PENDING).forEach(event -> {
        kafkaTemplate.send(event.topic, event.key, event.payload);
        event.setStatus(PUBLISHED);
        outboxRepo.save(event);
    });
}
```

If the relay crashes, it restarts and re-publishes — no events lost.

### CDC with Debezium (production approach)

Instead of a polling relay, **Debezium** reads the PostgreSQL **Write-Ahead Log (WAL)** — the binary log of every committed change — and streams it to Kafka in real time.

```
  Application code commits transaction
          │
          ▼
  ┌──────────────────────────────────────┐
  │  PostgreSQL WAL (Write-Ahead Log)    │
  │                                      │
  │  Each row change is recorded here    │
  │  before the transaction completes.   │
  │  Used for crash recovery + replication│
  └───────────────┬──────────────────────┘
                  │  wal_level = logical
                  │  (enables logical decoding — row-level change events)
                  ▼
  ┌──────────────────────────────────────┐
  │  Debezium PostgreSQL Connector       │
  │  (runs inside Kafka Connect)         │
  │                                      │
  │  1. Creates a replication slot       │
  │     → PostgreSQL holds WAL until     │
  │       Debezium confirms it's read    │
  │  2. Streams WAL as change events     │
  │  3. Filters to outbox_events table   │
  │  4. Applies Outbox Event Router:     │
  │     aggregate_type → Kafka topic     │
  │     aggregate_id   → message key     │
  │     payload        → message value   │
  └───────────────┬──────────────────────┘
                  │
                  ▼
  ┌──────────────────────────────────────┐
  │  Kafka topic (routed by aggregate_type)│
  │  key = aggregate_id = orderId/sagaId  │
  └──────────────────────────────────────┘
```

### Polling relay vs Debezium

| | Polling Relay | Debezium CDC |
|---|---|---|
| Latency | 1s (poll interval) | < 100ms (WAL stream) |
| Complexity | Simple Java code | Kafka Connect + connector config |
| Works when Kafka is down | Accumulates in DB ✓ | Accumulates in WAL ✓ |
| Multiple replicas | Need leader election | Single connector handles it |
| WAL expiry risk | None | Yes — connector must stay healthy |
| Visibility | Easy to query outbox table | Connector status via REST API |

### How Debezium connector fails silently (and how to detect it)

The connector task can enter `FAILED` state while Kafka Connect itself returns HTTP 200 on `/health`. Events stop flowing but no exception is thrown anywhere. Detection requires active monitoring:

```bash
# Check connector state — look for task.state != RUNNING
curl http://localhost:8083/connectors/saga-outbox-connector/status

# Response when healthy:
# { "connector": { "state": "RUNNING" }, "tasks": [{ "state": "RUNNING" }] }

# Response when failed:
# { "connector": { "state": "RUNNING" }, "tasks": [{ "state": "FAILED" }] }

# Fix: restart the failed task
curl -X POST http://localhost:8083/connectors/saga-outbox-connector/tasks/0/restart
```

Alert when: outbox `pendingCount > 0` for more than 30 seconds.

---

## Key Patterns Implemented

### Idempotency — safe Kafka replay

Kafka guarantees **at-least-once delivery**. The same message may arrive twice (network timeout, consumer rebalance). Without idempotency, this causes double charges, double reservations.

Every saga step checks an `idempotency_records` table before acting:

```java
// In InventoryCommandListener (inventory-service)
private void handleReserve(String sagaId, String orderId, ...) {
    String key = "INV_RESERVE:" + orderId;

    if (idempotency.alreadyProcessed(key)) {
        // Already done. Don't reserve again.
        // BUT: return the ACTUAL result from DB (not hardcoded true)
        boolean wasReserved = reservationRepo.findByOrderId(orderId)
            .stream().anyMatch(r -> "RESERVED".equals(r.getStatus()));
        reply(sagaId, "INVENTORY_RESERVED", wasReserved, ...);
        return;
    }

    // First time — do the actual work
    reserveStock(...);
    idempotency.markProcessed(key);  // UNIQUE constraint prevents race condition
    reply(sagaId, "INVENTORY_RESERVED", true, ...);
}
```

The `idempotency_records.idempotency_key` column has a `UNIQUE` constraint — even if two threads race, the DB guarantees only one insert succeeds.

Key insight: on replay, we look up what **actually happened** in the DB, not hardcode `true`. If the original reservation failed (out of stock), a replay correctly returns `false` again.

### Saga state machine

All saga state lives in the `saga_instances` table. The orchestrator is **completely stateless** — it loads state from DB, transitions it, saves it back. If it crashes mid-saga, the next Kafka message finds the same row and resumes correctly.

```
            POST /api/orders
                  │
                  ▼
       INVENTORY_RESERVING ──────────── saga_instances row created
              │        │
        [reserved]  [no stock]
              │        │
              ▼        ▼
  PAYMENT_PROCESSING  FAILED ────────── order CANCELLED
        │        │
   [charged]  [declined]
        │        │
        ▼        ▼
   COMPLETED  COMPENSATING ──────────── RELEASE command sent
                  │
            [stock released]
                  │
                  ▼
        FAILED_COMPENSATED ──────────── order CANCELLED, stock restored
```

### Partition key → ordered delivery

Kafka distributes messages across partitions using a hash of the message key. By using `sagaId` as the key:

- All 3 messages for saga `abc-123` go to **partition 2** (for example)
- All 3 are consumed **in order** by the orchestrator
- No risk of RELEASE arriving before RESERVE in the orchestrator

Without a key (null key), messages are round-robin distributed — RELEASE could arrive before RESERVE, causing uncompensable state.

---

## Troubleshooting

### Container won't start — check logs

```bash
docker compose logs order-service
docker compose logs inventory-service
docker compose logs payment-service
```

### `Connection refused` on port 8081/8082/8083

Services are still starting. Wait ~30s after `docker compose up -d` and check `docker compose ps` for `healthy` status.

### Saga stuck in `INVENTORY_RESERVING`

Inventory service may not be running or not connected to Kafka. Check:
```bash
docker compose ps inventory-service          # should show "healthy"
docker compose logs inventory-service        # look for "partitions assigned"
```

### Order stays `PENDING` forever

The orchestrator isn't receiving the reply. Check:
```bash
# Are topics being produced to?
# Open http://localhost:8090 → Topics → saga.orch.saga-replies
# Are there messages there?

# Are consumer groups caught up?
# Kafka UI → Consumer Groups → order-saga-orchestrator → lag should be 0
```

### `no main manifest attribute` when starting container

The JAR was built without the Spring Boot repackage goal. Run:
```bash
mvn package -DskipTests
docker compose up --build -d
```

### `HikariPool - Connection is not available` on startup

PostgreSQL healthcheck passed but wasn't fully ready when Flyway ran. The service will retry automatically. If it keeps failing:
```bash
docker compose restart order-service
```

### Fresh start (wipe all data)

```bash
docker compose down -v       # removes postgres_data volume
mvn package -DskipTests
docker compose up --build -d
```

---

## Project Structure

```
saga-microservices/
│
├── docker-compose.yml           7 containers: zookeeper, kafka, kafka-ui,
│                                postgres, order-service, inventory-service,
│                                payment-service
│
├── init-databases.sql           Runs on postgres first boot:
│                                CREATE DATABASE order_db / inventory_db / payment_db
│
├── pom.xml                      Parent POM — Spring Boot BOM, shared Java version
│
├── demo.sh                      Runs all 4 scenarios with colored output
│                                (good for recording a demo GIF)
│
├── order-service/               Port 8081 — owns orders + runs saga orchestrator
│   ├── Dockerfile               FROM eclipse-temurin:21-jre-jammy + COPY target/*.jar
│   ├── .dockerignore            Excludes src/, pom.xml from build context
│   ├── pom.xml
│   └── src/main/
│       ├── java/.../order/
│       │   ├── OrderServiceApplication.java
│       │   ├── config/
│       │   │   └── KafkaTopics.java       topic name constants
│       │   ├── controller/
│       │   │   └── OrderController.java   POST /api/orders, GET /api/orders/{id}
│       │   ├── domain/
│       │   │   ├── Order.java             @Entity — orders table
│       │   │   ├── OrderItem.java         @Entity — order_items table
│       │   │   ├── OrderStatus.java       PENDING/COMPLETED/CANCELLED enum
│       │   │   └── OrderRepository.java   JPA
│       │   └── saga/
│       │       ├── SagaState.java         state machine enum
│       │       ├── SagaInstance.java      @Entity — saga_instances table
│       │       ├── SagaInstanceRepository.java
│       │       ├── IdempotencyRepository.java  entity + JPA repo + service
│       │       └── OrderSagaOrchestrator.java  @KafkaListener — drives the saga
│       └── resources/
│           ├── application.yml
│           └── db/migration/V1__init.sql
│
├── inventory-service/           Port 8082 — owns stock levels + reservations
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── pom.xml
│   └── src/main/
│       ├── java/.../inventory/
│       │   ├── InventoryServiceApplication.java
│       │   ├── config/KafkaTopics.java
│       │   └── domain/
│       │       ├── InventoryItem.java              stock levels
│       │       ├── InventoryReservation.java        reservation records
│       │       ├── InventoryItemRepository.java     with @Lock(PESSIMISTIC_WRITE)
│       │       ├── InventoryReservationRepository.java
│       │       ├── InventoryCommandListener.java   handles RESERVE + RELEASE
│       │       ├── InventoryController.java        GET /api/inventory
│       │       ├── IdempotencyStore.java           deduplication
│       │       └── IdempotencyRecord.java
│       └── resources/
│           ├── application.yml
│           └── db/migration/
│               ├── V1__init.sql
│               └── V2__seed_inventory.sql   inserts 5 test products
│
└── payment-service/             Port 8083 — owns payment records
    ├── Dockerfile
    ├── .dockerignore
    ├── pom.xml
    └── src/main/
        ├── java/.../payment/
        │   ├── PaymentServiceApplication.java
        │   ├── config/KafkaTopics.java
        │   └── domain/
        │       ├── Payment.java                   payment records
        │       ├── PaymentRepository.java
        │       ├── PaymentCommandListener.java    handles CHARGE + REFUND
        │       │   decline rules: customerId contains "declined"
        │       │                  OR amount >= $5000
        │       ├── PaymentController.java         GET /api/payments
        │       ├── IdempotencyStore.java
        │       └── IdempotencyRecord.java
        └── resources/
            ├── application.yml
            └── db/migration/V1__init.sql
```
