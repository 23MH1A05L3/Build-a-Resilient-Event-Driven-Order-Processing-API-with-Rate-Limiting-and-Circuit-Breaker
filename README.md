# Resilient Event-Driven Order Processing API

A backend system for an e-commerce platform's order processing pipeline,
built around an **event-driven architecture** with **rate limiting** and a
**circuit breaker** to keep order intake highly available and protected
from cascading failures, even when downstream dependencies are slow or
unavailable.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Resilience Patterns](#resilience-patterns)
- [Event Contract](#event-contract)
- [Database Schema](#database-schema)
- [Idempotency](#idempotency)
- [Dead Letter Queue](#dead-letter-queue)
- [Testing](#testing)
- [Manually Exercising Resilience Behavior](#manually-exercising-resilience-behavior)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)

---

## Overview

The system separates the **acknowledgement** of an order from its
**eventual persistence and inventory management**:

1. A client calls `POST /api/v1/orders`. The API validates the request,
   checks rate limits and circuit breaker health, publishes an
   `OrderCreated` event to a message queue, and immediately returns
   `202 Accepted` with an `order_id` — without waiting on the database.
2. A separate **consumer service** subscribes to `OrderCreated` events,
   persists the order, deducts inventory transactionally, and updates the
   order's final status (`completed` or `failed`).
3. Clients can poll `GET /api/v1/orders/{order_id}` to see the order reach
   its final state (eventual consistency).

This decouples request intake from processing, allows each service to
scale and fail independently, and demonstrates production-grade resilience
patterns for a microservices environment.

---

## Architecture

```
                    ┌──────────────┐
   POST /orders     │              │   OrderCreated event    ┌──────────────────┐
  ─────────────────▶│  api-service │─────────────────────────▶│  order_events    │
                     │  (FastAPI)   │   (exchange: topic)      │  (RabbitMQ)      │
   GET /orders/:id   │  rate limit  │                          └────────┬─────────┘
  ─────────────────▶│  circuit     │                                   │ routing key: order.created
   GET /products/:id│  breaker     │                                   ▼
  ─────────────────▶└──────┬───────┘                    ┌───────────────────────────┐
                            │ read-only                   │ order_processing_queue    │
                            ▼                             └────────────┬───────────────┘
                    ┌──────────────┐                                  │ consume
                    │              │◀─────────────────────  ┌─────────▼─────────┐
                    │  mysql_db    │   writes (orders,        │ consumer-service │
                    │  (MySQL 8)   │   inventory, idempotency │  - create order  │
                    │              │   ledger)                │  - deduct stock  │
                    └──────────────┘                          │  - update status │
                                                                │  - idempotent    │
                                        failed after 3 tries    └────────┬─────────┘
                                        ┌───────────────────────────────┘
                                        ▼
                              ┌────────────────────────┐
                              │ order_processing_dlq    │
                              │ (via order_events_dlx)  │
                              └────────────────────────┘
```

**Services:**

| Service | Responsibility | Tech |
|---|---|---|
| `api-service` | Accept orders, validate, rate-limit, publish events, expose read endpoints | FastAPI, Pika, PyMySQL |
| `consumer-service` | Consume events, persist orders, manage inventory, idempotency, DLQ | Pika, PyMySQL |
| `mysql_db` | Persistent storage | MySQL 8.0 |
| `rabbitmq` | Message broker (pub/sub) | RabbitMQ 3 (management UI enabled) |

---

## Project Structure

```
.
├── .env.example
├── docker-compose.yml
├── README.md
├── sql/
│   └── init.sql                     # schema + seed data, auto-applied on first container start
└── services/
    ├── api-service/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── src/
    │   │   ├── main.py              # FastAPI app entrypoint
    │   │   ├── db.py                # MySQL connection helper (reads)
    │   │   ├── publisher.py         # RabbitMQ event publisher
    │   │   ├── routes/
    │   │   │   ├── order_routes.py  # POST /orders, GET /orders/{id}
    │   │   │   └── product_routes.py# GET /products/{id}
    │   │   ├── schemas/
    │   │   │   └── order_schema.py  # Pydantic request/response models
    │   │   └── utils/
    │   │       ├── rate_limiter.py  # sliding-window limiter
    │   │       └── circuit_breaker.py
    │   └── tests/
    │       └── test_api.py
    └── consumer-service/
        ├── Dockerfile
        ├── requirements.txt
        ├── src/
        │   ├── main.py              # consumer entrypoint
        │   ├── consumers/
        │   │   └── order_consumer.py# consume loop, DLQ wiring, retries
        │   └── database/
        │       ├── db.py            # MySQL connection helper (writes)
        │       └── models.py        # create_order_in_db, deduct_inventory, update_order_status
        └── tests/
            └── test_consumer.py
```

---

## Prerequisites

- Docker Engine 20.10+
- Docker Compose v2 (`docker compose` or `docker-compose`)
- Ports `8000`, `3306`, `5672`, `15672` free on the host

No local Python installation is required to run the stack — everything is
containerized. Python 3.11 is only needed if you want to run the unit
tests outside Docker.

---

## Quick Start

```bash
# 1. Clone/unzip the project, then from the project root:
cp .env.example .env

# 2. Build and start everything (MySQL, RabbitMQ, API, consumer)
docker-compose up --build

# 3. Wait for all services to report healthy, then verify:
curl http://localhost:8000/health
```

- **API base URL:** `http://localhost:8000`
- **RabbitMQ management UI:** `http://localhost:15672` (credentials from `.env`, default `guest`/`guest`)
- **MySQL:** `localhost:3306` (credentials from `.env`)

To stop and remove containers (keeping the DB volume):

```bash
docker-compose down
```

To also wipe persisted data:

```bash
docker-compose down -v
```

---

## API Reference

### `POST /api/v1/orders`

Creates an order asynchronously.

**Request body**
```json
{
  "product_id": "prod-101",
  "quantity": 2,
  "customer_id": "cust-1"
}
```

**Responses**

| Status | Condition | Body |
|---|---|---|
| `202 Accepted` | Event published successfully | `{"message": "Order creation initiated", "order_id": "<uuid>"}` |
| `400 Bad Request` | Missing/invalid fields | `{"error": "Validation failed", ...}` |
| `429 Too Many Requests` | > 10 requests/min from this IP | `{"error": "Rate limit exceeded. Try again in X seconds."}` |
| `503 Service Unavailable` | Circuit breaker OPEN (message queue unhealthy) | `{"error": "Service currently unavailable due to external dependency issues."}` |

**Example**
```bash
curl -i -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "prod-101", "quantity": 2, "customer_id": "cust-1"}'
```

### `GET /api/v1/orders/{order_id}`

Retrieves the current status of an order (eventually consistent — the
consumer may still be processing it).

| Status | Body |
|---|---|
| `200 OK` | `{"order_id": "...", "product_id": "...", "quantity": 2, "customer_id": "...", "status": "pending\|completed\|failed", "error_message": null}` |
| `404 Not Found` | `{"error": "Order not found"}` |

### `GET /api/v1/products/{product_id}`

| Status | Body |
|---|---|
| `200 OK` | `{"product_id": "...", "name": "...", "price": 1200.00, "stock_quantity": 50}` |
| `404 Not Found` | `{"error": "Product not found"}` |

### `GET /health`

Liveness probe used by Docker's healthcheck. Returns `{"status": "ok"}`.

---

## Resilience Patterns

### Rate Limiting

- **Algorithm:** sliding window, per client IP.
- **Limit:** 10 requests / 60 seconds (configurable via `RATE_LIMIT_MAX` /
  `RATE_LIMIT_WINDOW_SECONDS`).
- Implemented in `utils/rate_limiter.py` with a `deque` timestamp window
  per key, guarded by a lock for thread safety under FastAPI's threaded
  request handling.
- Exceeding the limit returns `429` immediately, before any downstream
  work (validation, publish) is attempted.

### Circuit Breaker

- **States:** `CLOSED → OPEN → HALF_OPEN → CLOSED` (or back to `OPEN` on a
  failed trial).
- **Trips open** after 5 failures within a 1-minute rolling window
  (`CB_FAILURE_THRESHOLD` / `CB_WINDOW_SECONDS`).
- **Recovery:** after 30 seconds (`CB_RECOVERY_SECONDS`) it moves to
  `HALF_OPEN` and allows a single trial request through; success closes
  the circuit, failure re-opens it.
- **Monitored dependency:** the RabbitMQ publish call in
  `publisher.publish_order_created`. A connection or publish failure
  records a breaker failure; a successful publish records a success.
- While `OPEN`, requests are rejected with `503` immediately — no wasted
  time on a doomed connection attempt — protecting the API from cascading
  latency.
- Implemented in `utils/circuit_breaker.py`, independent of any specific
  transport so it could equally wrap an HTTP call to another dependency.

---

## Event Contract

**Exchange:** `order_events` (topic, durable)
**Routing key:** `order.created`
**Queue:** `order_processing_queue` (durable, bound to `order_events` with
key `order.created`)

**Payload (`OrderCreated`)**
```json
{
  "event_id": "uuid",
  "event_type": "OrderCreated",
  "order_id": "uuid",
  "product_id": "prod-101",
  "quantity": 2,
  "customer_id": "cust-1",
  "timestamp": "2026-07-18T09:00:00+00:00"
}
```

Messages are published with `delivery_mode=2` (persistent) and a
`message_id` set to `event_id`, so they survive a RabbitMQ restart while
awaiting consumption.

---

## Database Schema

Applied automatically by `sql/init.sql` on the MySQL container's first
startup (via `docker-entrypoint-initdb.d`):

- **`products`** — `id`, `name`, `price`, `stock_quantity`. Seeded with
  `prod-101` (Laptop Pro), `prod-102` (Mechanical Keyboard), `prod-103`
  (Gaming Mouse).
- **`orders`** — `id`, `product_id` (FK → `products`), `quantity`,
  `customer_id`, `status` (`pending` / `completed` / `failed`),
  `error_message`, `created_at`, `updated_at`. Indexed on `status` and
  `customer_id`.
- **`processed_events`** — `event_id` (PK), `order_id`, `processed_at`.
  The idempotency ledger described below.

---

## Idempotency

RabbitMQ (like most brokers) offers **at-least-once** delivery, so the
same `OrderCreated` event can be redelivered (e.g. after a consumer crash
before acking). The consumer guards against duplicate side effects with a
step-by-step, idempotent flow in `process_order_event`:

1. **Check** `processed_events` for the incoming `event_id`. If present,
   the event was already fully handled — skip all further work and ack.
2. **Insert** the order row with `INSERT IGNORE` (idempotent — a repeat
   insert for the same `order_id` is a no-op).
3. **Deduct inventory** via a single conditional `UPDATE ... WHERE
   stock_quantity >= quantity`, which is naturally safe against double
   deduction as long as step 1 caught the duplicate; the row's atomic
   `UPDATE` also prevents a race between concurrent consumers from
   over-selling stock.
4. **Record** the `event_id` in `processed_events` only after the order
   has reached a final status, closing the idempotency window.

---

## Dead Letter Queue

- `order_processing_queue` is declared with
  `x-dead-letter-exchange: order_events_dlx`.
- On an unexpected processing error, the consumer inspects the message's
  `x-death` header (populated by RabbitMQ on redelivery) to count prior
  delivery attempts.
  - **Attempts < 3:** `basic_nack(requeue=True)` — retry.
  - **Attempts ≥ 3:** `basic_nack(requeue=False)` — RabbitMQ routes the
    message to `order_events_dlx` → `order_processing_dlq` for manual
    inspection/replay, preventing a poison message from blocking the
    queue indefinitely.
- Business-level failures (e.g. insufficient stock) are **not** treated as
  processing errors — the order is marked `failed` with an
  `error_message` and the message is acked normally, since the event was
  handled successfully (just with a business outcome the customer needs
  to see).

---

## Testing

Both services have unit test suites covering the resilience logic and
business rules using `pytest` and `unittest.mock` (no live infrastructure
required).

```bash
# api-service
cd services/api-service
pip install -r requirements.txt pytest
PYTHONPATH=src pytest tests/ -v

# consumer-service
cd services/consumer-service
pip install -r requirements.txt pytest
PYTHONPATH=src pytest tests/ -v
```

**Coverage includes:**
- Rate limiter: per-key limits, independence across keys, sliding-window
  expiry.
- Circuit breaker: opens at threshold, transitions to `HALF_OPEN` after
  cooldown, closes on trial success, re-opens on trial failure.
- `create_order_handler`: validation errors (`400`), successful publish
  (`202`), publish failure surfaced as `503`.
- `process_order_event`: successful completion, insufficient-stock
  failure path, idempotent skip on duplicate `event_id`.

All 13 tests pass (10 in `api-service`, 3 in `consumer-service`).

---

## Manually Exercising Resilience Behavior

**Rate limiting** — fire more than 10 requests in under a minute from the
same origin:
```bash
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/orders \
    -H "Content-Type: application/json" \
    -d '{"product_id":"prod-101","quantity":1,"customer_id":"cust-load-test"}'
done
# Expect the first 10 to return 202, the rest 429
```

**Circuit breaker** — take the message broker down and observe the
breaker trip:
```bash
docker-compose stop rabbitmq
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/orders \
    -H "Content-Type: application/json" \
    -d '{"product_id":"prod-101","quantity":1,"customer_id":"cust-cb-test"}'
done
# Expect the first 5 to fail with 503 while accumulating breaker failures,
# then subsequent requests to short-circuit immediately with 503

docker-compose start rabbitmq
sleep 30   # CB_RECOVERY_SECONDS
curl -i -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id":"prod-101","quantity":1,"customer_id":"cust-cb-test"}'
# Expect 202 once the breaker's HALF_OPEN trial succeeds
```

**Insufficient stock / DLQ** — order more than the available stock, then
inspect the `order_processing_dlq` in the RabbitMQ management UI for any
message that fails processing repeatedly.

---

## Design Decisions & Trade-offs

- **In-process rate limiter / circuit breaker vs. a library:** implemented
  from scratch for transparency and to avoid an extra dependency; both are
  in-memory, so state is per-container (fine for a single API replica —
  horizontally scaling `api-service` would need a shared store such as
  Redis to coordinate limits across instances).
- **MySQL over NoSQL:** chosen per the task's recommendation and because
  order/inventory consistency benefits from relational constraints and
  atomic conditional updates.
- **Topic exchange over direct/fanout:** allows future event types (e.g.
  `OrderCancelled`) to reuse the same exchange with different routing
  keys without restructuring the topology.
- **DLQ via broker-native dead-lettering** rather than an
  application-managed retry table: keeps failure handling declarative and
  visible in the RabbitMQ management UI.
- **Read endpoints query MySQL directly from `api-service`:** this keeps
  the read path simple and low-latency; the API service is otherwise the
  write-initiator only (via the queue), so it holds no business logic
  around order state — it just published the event.

---

## Environment Variables

Defined in `.env.example` (copy to `.env` before running):

| Variable | Description |
|---|---|
| `DB_ROOT_PASSWORD` | MySQL root password |
| `DB_NAME` | Database name |
| `DB_USER` / `DB_PASSWORD` | Application DB credentials |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | Broker credentials |

Additional tunables (set directly in `docker-compose.yml` under
`api-service.environment`):

| Variable | Default | Description |
|---|---|---|
| `RATE_LIMIT_MAX` | `10` | Max requests per window per IP |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate limit window size |
| `CB_FAILURE_THRESHOLD` | `5` | Failures before the breaker opens |
| `CB_WINDOW_SECONDS` | `60` | Rolling window for counting failures |
| `CB_RECOVERY_SECONDS` | `30` | Cooldown before a HALF_OPEN trial |

---

## Troubleshooting

- **`api-service` unhealthy / can't connect to MySQL:** `docker-compose`
  waits for `mysql_db`'s healthcheck before starting `api-service` —
  check `docker-compose logs mysql_db` if it stays unhealthy.
- **Orders stuck in `pending`:** check `docker-compose logs
  consumer-service`; also check the RabbitMQ management UI for messages
  stuck in `order_processing_dlq`.
- **`429` immediately on the first request:** confirm you're not behind a
  proxy that reuses the same source IP for many test clients — the limiter
  keys on the request's client IP.
- **Port conflicts:** adjust the host-side ports in `docker-compose.yml`
  (e.g. `"8001:8000"`) if `8000`/`3306`/`5672`/`15672` are already in use
  locally.
