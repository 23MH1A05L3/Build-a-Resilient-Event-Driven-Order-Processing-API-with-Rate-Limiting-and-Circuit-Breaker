import os
import uuid
import time
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from schemas.order_schema import OrderCreateRequest
from utils.rate_limiter import RateLimiter
from utils.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpenError
from db import get_connection
from publisher import publish_order_created

logger = logging.getLogger("api-service.orders")
router = APIRouter()

RATE_LIMIT_MAX = int(os.environ.get("RATE_LIMIT_MAX", 10))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", 60))
CB_FAILURE_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", 5))
CB_WINDOW_SECONDS = int(os.environ.get("CB_WINDOW_SECONDS", 60))
CB_RECOVERY_SECONDS = int(os.environ.get("CB_RECOVERY_SECONDS", 30))

rate_limiter = RateLimiter(limit=RATE_LIMIT_MAX, window_seconds=RATE_LIMIT_WINDOW_SECONDS)


def _monitor(event: str, state: CircuitState):
    logger.warning("Circuit breaker state change: %s", state.value)


circuit_breaker = CircuitBreaker(
    failure_threshold=CB_FAILURE_THRESHOLD,
    recovery_timeout_seconds=CB_RECOVERY_SECONDS,
    failure_window_seconds=CB_WINDOW_SECONDS,
    monitor_function=_monitor,
)


def create_order_handler(request_data: dict, client_ip: str):
    """Handles POST /api/v1/orders, publishes event. Returns (status_code, body)."""

    if not rate_limiter.allow_request(client_ip):
        retry_after = rate_limiter.retry_after(client_ip)
        return 429, {"error": f"Rate limit exceeded. Try again in {retry_after} seconds."}

    try:
        order = OrderCreateRequest(**request_data)
    except Exception as exc:
        return 400, {"error": "Validation failed", "details": str(exc)}

    if not circuit_breaker.allow_request():
        return 503, {"error": "Service currently unavailable due to external dependency issues."}

    order_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "order_id": order_id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "customer_id": order.customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        publish_order_created(event)
        circuit_breaker.record_success()
    except Exception as exc:
        circuit_breaker.record_failure()
        logger.error("Failed to publish OrderCreated event: %s", exc)
        return 503, {"error": "Service currently unavailable due to external dependency issues."}

    return 202, {"message": "Order creation initiated", "order_id": order_id}


def get_order_status_handler(order_id: str):
    """Retrieves order status. Returns (status_code, body)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id AS order_id, product_id, quantity, customer_id, status, error_message "
                "FROM orders WHERE id = %s",
                (order_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return 404, {"error": "Order not found"}
    return 200, row


@router.post("/api/v1/orders")
async def create_order(request: Request):
    body = await request.json()
    client_ip = request.client.host if request.client else "unknown"
    status_code, payload = create_order_handler(body, client_ip)
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/v1/orders/{order_id}")
async def get_order(order_id: str):
    status_code, payload = get_order_status_handler(order_id)
    return JSONResponse(status_code=status_code, content=payload)
