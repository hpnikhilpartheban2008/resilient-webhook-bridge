"""Local dashboard and webhook HTTP ingress for Resilient Webhook Bridge."""

from __future__ import annotations

import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from resilient_webhook_bridge import WebhookBridge

HOST = os.getenv("BRIDGE_HOST", "127.0.0.1")
PORT = int(os.getenv("BRIDGE_PORT", "8080"))
SECRET = os.getenv("WEBHOOK_SECRET", "local-development-secret")
MAX_BODY = 1_048_576
bridge = WebhookBridge(SECRET, dedupe_window=float(os.getenv("DEDUPE_WINDOW_SECONDS", "300")))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("webhook-bridge")


class Handler(BaseHTTPRequestHandler):
    server_version = "ResilientWebhookBridge/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _send(self, status: int, body: bytes, content_type: str = "application/json; charset=utf-8") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: object) -> None:
        self._send(status, json.dumps(value, separators=(",", ":")).encode("utf-8"))

    def _read_json(self) -> dict | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                return None
            value = json.loads(self.rfile.read(length))
            return value if isinstance(value, dict) else None
        except (ValueError, json.JSONDecodeError):
            return None

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        route = urlparse(self.path).path
        if route == "/":
            page_path = Path(__file__).with_name("dashboard.html")
            if not page_path.is_file():
                page_path = Path(sys.prefix) / "dashboard.html"
            page = page_path.read_bytes()
            self._send(200, page, "text/html; charset=utf-8")
        elif route == "/api/state":
            self._json(200, bridge.snapshot())
        elif route == "/health":
            self._json(200, {"status": "ok"})
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        route = urlparse(self.path).path
        if route == "/webhook":
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            if length <= 0 or length > MAX_BODY:
                self._json(413, {"error": "invalid_content_length"})
                return
            payload = self.rfile.read(length)
            try:
                json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._json(400, {"error": "invalid_json"})
                return
            event_id = self.headers.get("X-Event-ID", "").strip()
            if not event_id or len(event_id) > 200:
                self._json(400, {"error": "missing_or_invalid_event_id"})
                return
            status, response = bridge.process(payload, self.headers.get("X-Hub-Signature-256"), event_id, self.client_address[0])
            self._json(status, response)
        elif route == "/api/outage":
            data = self._read_json()
            if data is None or not isinstance(data.get("enabled"), bool):
                self._json(400, {"error": "expected_boolean_enabled"})
                return
            bridge.set_outage(data["enabled"])
            self._json(200, {"outage": data["enabled"]})
        else:
            self._json(404, {"error": "not_found"})


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    logger.info("Dashboard and webhook ingress listening on http://%s:%d", HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
