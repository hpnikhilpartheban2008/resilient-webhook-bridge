"""Unit tests for the webhook bridge's security and reliability behavior."""

import unittest

from resilient_webhook_bridge import WebhookBridge


class WebhookBridgeTests(unittest.TestCase):
    def setUp(self):
        self.sleeps = []
        self.bridge = WebhookBridge("test-secret", sleeper=self.sleeps.append)
        self.payload = b'{"type":"invoice.paid","data":{"id":"inv_1"}}'

    def signature(self, payload=None):
        return self.bridge.signature(payload or self.payload, "test-secret")

    def test_valid_payload_authenticates_and_dispatches(self):
        status, body = self.bridge.process(self.payload, self.signature(), "evt_1")
        self.assertEqual(status, 202)
        self.assertEqual(body["status"], "delivered")
        self.assertEqual(body["attempts"], 1)
        self.assertEqual(self.bridge.snapshot()["stats"]["authenticated"], 1)

    def test_tampered_signature_is_rejected(self):
        signature = self.signature()
        tampered = signature[:-1] + ("0" if signature[-1] != "0" else "1")
        status, body = self.bridge.process(self.payload, tampered, "evt_bad")
        self.assertEqual(status, 401)
        self.assertEqual(body["reason"], "invalid_signature")
        self.assertEqual(self.bridge.snapshot()["stats"]["authenticated"], 0)

    def test_duplicate_event_is_ignored_with_http_200(self):
        self.bridge.process(self.payload, self.signature(), "evt_repeat")
        status, body = self.bridge.process(self.payload, self.signature(), "evt_repeat")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "duplicate_ignored")
        self.assertEqual(self.bridge.snapshot()["stats"]["dropped"], 1)

    def test_retry_exhaustion_sends_event_to_dlq(self):
        bridge = WebhookBridge("test-secret", downstream=lambda: False, sleeper=self.sleeps.append)
        status, body = bridge.process(self.payload, bridge.signature(self.payload, "test-secret"), "evt_offline")
        self.assertEqual(status, 503)
        self.assertEqual(body["attempts"], 4)  # Initial attempt plus three retries.
        self.assertEqual(self.sleeps, [1.0, 2.0, 4.0])
        state = bridge.snapshot()
        self.assertEqual(state["stats"]["retried"], 3)
        self.assertEqual(state["stats"]["dead_lettered"], 1)
        self.assertEqual(state["dlq"][0]["event_id"], "evt_offline")


if __name__ == "__main__":
    unittest.main()
