import time
from typing import Any, Callable, Optional


class TTLCache:
    """
    Very small in-process TTL cache.
    - Suitable for typeahead/search endpoints
    - Per-process (per Gunicorn worker)
    """

    def __init__(self, ttl_seconds: int = 30, max_items: int = 512):
        self.ttl = int(ttl_seconds)
        self.max = int(max_items)
        self._store = {}  # key -> (expires_at, value)

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        item = self._store.get(key)
        if not item:
            return None
        expires_at, val = item
        if expires_at < now:
            self._store.pop(key, None)
            return None
        return val

    def set(self, key: str, val: Any):
        now = time.time()
        if len(self._store) >= self.max:
            # Evict expired first; if still too big, evict arbitrary oldest-ish by scanning
            self.prune()
            if len(self._store) >= self.max:
                # pop one arbitrary key
                self._store.pop(next(iter(self._store.keys())), None)

        self._store[key] = (now + self.ttl, val)

    def prune(self):
        now = time.time()
        dead = [k for k, (exp, _) in self._store.items() if exp < now]
        for k in dead:
            self._store.pop(k, None)

    def get_or_set(self, key: str, fn: Callable[[], Any]) -> Any:
        val = self.get(key)
        if val is not None:
            return val
        val = fn()
        self.set(key, val)
        return val
