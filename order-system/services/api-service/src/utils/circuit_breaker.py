import time
import threading
from collections import deque
from enum import Enum
from typing import Callable, Optional


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreaker:
    """
    Tracks the health of an external dependency (e.g. the message queue
    connection). Opens after `failure_threshold` failures within
    `failure_window_seconds`, then moves to HALF_OPEN after
    `recovery_timeout_seconds` to test recovery. A single success in
    HALF_OPEN closes the circuit; a failure re-opens it.
    """

    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout_seconds: int,
        failure_window_seconds: int,
        monitor_function: Optional[Callable[[str, "CircuitState"], None]] = None,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.failure_window_seconds = failure_window_seconds
        self.monitor_function = monitor_function

        self._state = CircuitState.CLOSED
        self._failures: deque = deque()
        self._opened_at: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state

    def _maybe_transition_to_half_open(self):
        if self._state == CircuitState.OPEN and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self.recovery_timeout_seconds:
                self._set_state(CircuitState.HALF_OPEN)

    def _set_state(self, new_state: CircuitState):
        if new_state != self._state:
            self._state = new_state
            if self.monitor_function:
                self.monitor_function("state_change", new_state)

    def allow_request(self) -> bool:
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state != CircuitState.OPEN

    def record_success(self):
        with self._lock:
            self._failures.clear()
            self._set_state(CircuitState.CLOSED)
            self._opened_at = None

    def record_failure(self):
        with self._lock:
            now = time.monotonic()
            self._failures.append(now)
            while self._failures and now - self._failures[0] > self.failure_window_seconds:
                self._failures.popleft()

            if self._state == CircuitState.HALF_OPEN:
                # Failed the trial request -> re-open immediately
                self._set_state(CircuitState.OPEN)
                self._opened_at = now
            elif len(self._failures) >= self.failure_threshold:
                self._set_state(CircuitState.OPEN)
                self._opened_at = now

    def call(self, func: Callable, *args, **kwargs):
        if not self.allow_request():
            raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        try:
            result = func(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        else:
            self.record_success()
            return result
