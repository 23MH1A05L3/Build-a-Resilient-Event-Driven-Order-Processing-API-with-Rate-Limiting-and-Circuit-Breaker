import time
import threading
from collections import defaultdict, deque


class RateLimiter:
    """
    Sliding-window rate limiter, keyed per client (e.g. IP address).
    Allows at most `limit` requests within any rolling `window_seconds`
    window for a given key.
    """

    def __init__(self, limit: int, window_seconds: int):
        self.limit = limit
        self.window_seconds = window_seconds
        self._requests: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow_request(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            q = self._requests[key]
            # Evict timestamps outside the sliding window
            while q and now - q[0] > self.window_seconds:
                q.popleft()

            if len(q) >= self.limit:
                return False

            q.append(now)
            return True

    def retry_after(self, key: str) -> int:
        """Seconds until the oldest request in the window expires."""
        with self._lock:
            q = self._requests[key]
            if not q:
                return 0
            elapsed = time.monotonic() - q[0]
            return max(0, int(self.window_seconds - elapsed) + 1)
