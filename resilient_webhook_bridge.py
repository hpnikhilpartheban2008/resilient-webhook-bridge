"""Core webhook verification, deduplication, dispatch and metrics."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class EventRecord:
    event_id: str
    source: str
    signature_status: str
    attempt_count: int
    latency_ms: float
    outcome: str
    received_at: float


class WebhookBridge:
    """Thread-safe in-memory webhook pipeline suitable for a local demo."""

    def __init__(
        self,
        secret: str,
        dedupe_window: float = 300.0,
        retry_delays: tuple[float, ...] = (1.0, 2.0, 4.0),
        downstream: Callable[[], bool] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not secret:
            raise ValueError("secret must not be empty")
        self.secret = secret.encode("utf-8")
        self.dedupe_window = dedupe_window
        self.retry_delays = retry_delays
        self.downstream = downstream or (lambda: True)
        self.sleeper = sleeper
        self.clock = clock
        self._lock = threading.RLock()
        self._seen: dict[str, float] = {}
        self._events: deque[EventRecord] = deque(maxlen=500)
        self._counts = {"received": 0, "authenticated": 0, "dropped": 0, "retried": 0, "dead_lettered": 0}
        self._outage = False
        self.dead_letter_queue: deque[dict] = deque(maxlen=500)

    @staticmethod
    def signature(payload: bytes, secret: str | bytes) -> str:
        key = secret.encode("utf-8") if isinstance(secret, str) else secret
        return "sha256=" + hmac.new(key, payload, hashlib.sha256).hexdigest()

    def set_outage(self, enabled: bool) -> None:
        with self._lock:
            self._outage = enabled

    def process(self, payload: bytes, signature: str | None, event_id: str, source: str = "webhook") -> tuple[int, dict]:
        started = time.perf_counter()
        with self._lock:
            self._counts["received"] += 1
        expected = self.signature(payload, self.secret)
        if not signature or not hmac.compare_digest(expected, signature.strip()):
            self._record(event_id, source, "invalid", 0, started, "rejected")
            return 401, {"status": "rejected", "reason": "invalid_signature"}

        now = self.clock()
        with self._lock:
            self._counts["authenticated"] += 1
            self._expire_seen(now)
            if event_id in self._seen:
                self._counts["dropped"] += 1
                self._append_event(EventRecord(event_id, source, "valid", 0, self._latency(started), "duplicate", time.time()))
                return 200, {"status": "duplicate_ignored", "event_id": event_id}
            self._seen[event_id] = now

        attempts = 0
        success = False
        # Initial attempt followed by at most three retries (default 1s, 2s, 4s).
        for retry_index in range(len(self.retry_delays) + 1):
            attempts += 1
            try:
                with self._lock:
                    forced_outage = self._outage
                success = not forced_outage and bool(self.downstream())
            except Exception:
                logger.exception("Downstream dispatch failed for event %s", event_id)
                success = False
            if success:
                break
            if retry_index < len(self.retry_delays):
                with self._lock:
                    self._counts["retried"] += 1
                self.sleeper(self.retry_delays[retry_index])

        if not success:
            with self._lock:
                self._counts["dead_lettered"] += 1
                self.dead_letter_queue.append({"event_id": event_id, "payload": payload.decode("utf-8", errors="replace"), "attempts": attempts, "failed_at": time.time()})
        outcome = "delivered" if success else "dead_lettered"
        self._record(event_id, source, "valid", attempts, started, outcome)
        return (202 if success else 503), {"status": outcome, "event_id": event_id, "attempts": attempts}

    def _expire_seen(self, now: float) -> None:
        expired = [key for key, seen_at in self._seen.items() if now - seen_at >= self.dedupe_window]
        for key in expired:
            del self._seen[key]

    @staticmethod
    def _latency(started: float) -> float:
        return round((time.perf_counter() - started) * 1000, 2)

    def _append_event(self, event: EventRecord) -> None:
        self._events.appendleft(event)

    def _record(self, event_id: str, source: str, signature_status: str, attempts: int, started: float, outcome: str) -> None:
        with self._lock:
            self._append_event(EventRecord(event_id, source, signature_status, attempts, self._latency(started), outcome, time.time()))

    def snapshot(self) -> dict:
        with self._lock:
            return {"stats": dict(self._counts), "events": [asdict(event) for event in self._events], "outage": self._outage, "dlq": list(self.dead_letter_queue)}


def parse_json_payload(payload: bytes) -> object:
    return json.loads(payload.decode("utf-8"))
