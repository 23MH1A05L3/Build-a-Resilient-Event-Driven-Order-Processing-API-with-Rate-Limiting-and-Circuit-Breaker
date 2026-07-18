import os
import sys
import time
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.rate_limiter import RateLimiter
from utils.circuit_breaker import CircuitBreaker, CircuitState


def test_rate_limiter_allows_up_to_limit():
    rl = RateLimiter(limit=3, window_seconds=60)
    assert rl.allow_request("ip1") is True
    assert rl.allow_request("ip1") is True
    assert rl.allow_request("ip1") is True
    assert rl.allow_request("ip1") is False


def test_rate_limiter_is_per_key():
    rl = RateLimiter(limit=1, window_seconds=60)
    assert rl.allow_request("ip1") is True
    assert rl.allow_request("ip2") is True
    assert rl.allow_request("ip1") is False


def test_rate_limiter_sliding_window_expires():
    rl = RateLimiter(limit=1, window_seconds=1)
    assert rl.allow_request("ip1") is True
    assert rl.allow_request("ip1") is False
    time.sleep(1.1)
    assert rl.allow_request("ip1") is True


def test_circuit_breaker_opens_after_threshold():
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout_seconds=30, failure_window_seconds=60)
    assert cb.state == CircuitState.CLOSED
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.CLOSED
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.allow_request() is False


def test_circuit_breaker_half_open_after_recovery():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout_seconds=1, failure_window_seconds=60)
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    time.sleep(1.1)
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.allow_request() is True


def test_circuit_breaker_closes_on_success_in_half_open():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout_seconds=1, failure_window_seconds=60)
    cb.record_failure()
    time.sleep(1.1)
    assert cb.state == CircuitState.HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitState.CLOSED


def test_circuit_breaker_reopens_on_failure_in_half_open():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout_seconds=1, failure_window_seconds=60)
    cb.record_failure()
    time.sleep(1.1)
    assert cb.state == CircuitState.HALF_OPEN
    cb.record_failure()
    assert cb.state == CircuitState.OPEN


def test_create_order_handler_validation_error():
    from routes.order_routes import create_order_handler
    status_code, body = create_order_handler({"product_id": "p1"}, "127.0.0.1")
    assert status_code == 400
    assert "error" in body


@patch("routes.order_routes.publish_order_created")
def test_create_order_handler_success(mock_publish):
    from routes import order_routes
    order_routes.rate_limiter = RateLimiter(limit=10, window_seconds=60)
    order_routes.circuit_breaker = CircuitBreaker(
        failure_threshold=5, recovery_timeout_seconds=30, failure_window_seconds=60
    )
    status_code, body = order_routes.create_order_handler(
        {"product_id": "prod-101", "quantity": 2, "customer_id": "cust-1"}, "127.0.0.1"
    )
    assert status_code == 202
    assert "order_id" in body
    mock_publish.assert_called_once()


@patch("routes.order_routes.publish_order_created", side_effect=Exception("mq down"))
def test_create_order_handler_publish_failure_returns_503(mock_publish):
    from routes import order_routes
    order_routes.rate_limiter = RateLimiter(limit=10, window_seconds=60)
    order_routes.circuit_breaker = CircuitBreaker(
        failure_threshold=5, recovery_timeout_seconds=30, failure_window_seconds=60
    )
    status_code, body = order_routes.create_order_handler(
        {"product_id": "prod-101", "quantity": 2, "customer_id": "cust-1"}, "127.0.0.1"
    )
    assert status_code == 503
