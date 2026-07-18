# Resilient Event-Driven Order Processing API

A backend system for an e-commerce platform's order processing, built with
an event-driven architecture, rate limiting, and a circuit breaker.

## Architecture

- **api-service** (FastAPI): Accepts order requests, applies a sliding-window
  rate limiter (10 req/min/IP) and a circuit breaker around the message
  publish call, and publishes `OrderCreated` events to RabbitMQ. Also
  exposes read endpoints for order status and product details.
- **consumer-service**: Subscribes to `OrderCreated` events, persists orders,
  deducts inventory transactionally, and updates order status. Includes
  idempotent processing (via a `processed_events` table), retries, and a
  Dead Letter Queue (DLQ) for messages that repeatedly fail.
- **mysql_db**: Persistent storage for `products`, `orders`, and
  `processed_events`, seeded automatically via `sql/init.sql`.
- **rabbitmq**: Message broker. Exchange `order_events` (topic) -> queue
  `order_processing_queue` (routing key `order.created`). Failed messages
  dead-letter to exchange `order_events_dlx` -> queue `order_processing_dlq`.

## Running

```bash
cp .env.example .env
docker-compose up --build
```

The API is available at `http://localhost:8000`.
RabbitMQ management UI: `http://localhost:15672` (user/pass from `.env`).

## Endpoints

- `POST /api/v1/orders` — create an order (`202`, `400`, `429`, `503`)
- `GET /api/v1/orders/{order_id}` — get order status
- `GET /api/v1/products/{product_id}` — get product details/stock

### Example

```bash
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "prod-101", "quantity": 2, "customer_id": "cust-1"}'
```

## Resilience Patterns

- **Rate limiting**: sliding window, 10 requests/minute per client IP,
  returns `429` with a retry-after hint when exceeded.
- **Circuit breaker**: CLOSED -> OPEN after 5 failures within 1 minute
  (returns `503`) -> HALF_OPEN after a 30s cooldown to test recovery ->
  CLOSED on success / OPEN again on failure. Wraps the RabbitMQ publish
  call in `api-service`.
- **Idempotency**: the consumer records each processed `event_id` in
  `processed_events` before considering the event complete, so redelivered
  messages (at-least-once delivery) are safely skipped.
- **DLQ**: messages are retried up to 3 delivery attempts (tracked via
  RabbitMQ's `x-death` header) before being routed to
  `order_processing_dlq` for manual inspection.

## Testing

```bash
cd services/api-service && pip install -r requirements.txt pytest && pytest tests/
cd services/consumer-service && pip install -r requirements.txt pytest && pytest tests/
```

## Simulating Circuit Breaker Behavior

Stop RabbitMQ (`docker-compose stop rabbitmq`) and issue 5+ order requests
within a minute — the circuit will open and subsequent requests will
immediately receive `503` instead of hanging on connection attempts. After
30 seconds it moves to HALF_OPEN and will test the connection again.
