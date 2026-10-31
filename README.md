# Resilient Webhook Bridge

A small, dependency-free Python service that accepts signed webhook requests, filters replayed event IDs, retries transient downstream failures, and exposes an operations dashboard. It is designed as a runnable reference implementation for the networking and reliability controls commonly used between webhook providers and internal services.

## The problem

Webhook delivery crosses trust and availability boundaries. Requests can be forged, providers may redeliver the same event after a timeout, and downstream services can become temporarily unreachable. Without controls at the receiver, that combination can cause spoofed actions, duplicate side effects, and lost work.

## Architecture

```text
Provider / simulator
        │ POST /webhook + event ID + HMAC-SHA256
        ▼
HTTP ingress (ThreadingHTTPServer, 127.0.0.1:8080)
        ├─ validate JSON, size, event ID
        ├─ authenticate signature with constant-time comparison
        ├─ deduplicate within configured TTL ── duplicate → 200 ignored
        └─ dispatch downstream
              ├─ success → 202 delivered
              └─ failure → retry after 1s, 2s, 4s → 503 + in-memory DLQ

Dashboard (/) ── live counters and event log via /api/state
```

The dashboard and ingress share a threaded local HTTP server. `resilient_webhook_bridge.py` contains the pipeline and can also be used independently of the dashboard. The downstream adapter is intentionally simulated: by default it succeeds, and the dashboard outage control makes dispatch fail so retry and dead-letter behavior can be observed.

## HMAC-SHA256 authentication

The sender computes `HMAC-SHA256(secret, exact_request_body_bytes)` and sends the lowercase hexadecimal digest in the `X-Hub-Signature-256` header with the conventional `sha256=` prefix. The receiver recomputes the digest over the raw bytes and compares it with `hmac.compare_digest`, avoiding a normal early-exit string comparison. A missing or invalid signature is rejected with HTTP 401 before dispatch.

Configure the shared key with `WEBHOOK_SECRET`. Keep it secret and use a different, high-entropy value in each real environment. The dashboard's Secret Key field is for its browser-side simulator; it must match `WEBHOOK_SECRET` for a valid request. The default key is only for local development.

## Idempotency and retries

The receiver requires a unique `X-Event-ID`. Authenticated IDs are retained in a thread-safe in-memory map for `DEDUPE_WINDOW_SECONDS` (default 300 seconds). A repeat within that window is immediately acknowledged with HTTP 200 and `duplicate_ignored`; no second downstream dispatch occurs. Expired IDs are removed when another authenticated request arrives.

Dispatch makes one initial attempt and then up to three retries with exponential delays of 1, 2, and 4 seconds. After the fourth failed attempt, the payload and failure details are added to the in-memory dead-letter queue and the request receives HTTP 503. The retry delay tuple and downstream adapter are injectable in Python for alternate integrations and fast tests.

## Dashboard

Visit [http://127.0.0.1:8080](http://127.0.0.1:8080). The dashboard includes event, authentication, duplicate, retry, and dead-letter counters, a recent event table, editable JSON/key/event ID fields, and controls for valid, duplicate, tampered-signature, and downstream-outage scenarios. Metrics refresh every 1.2 seconds.

The demo uses in-memory state with a bounded recent event log and DLQ. State is lost on restart and is not a durable production queue. Before production use, connect durable storage for idempotency and dead-letter records, configure TLS and network access controls at the deployment boundary, add downstream timeouts and a real adapter, and define key rotation and retention policies.

## Setup

Python 3.10 or newer is required. There are no third-party runtime dependencies.

```powershell
python dashboard.py
```

Optional settings:

```powershell
$env:WEBHOOK_SECRET = "replace-with-a-long-random-secret"
$env:DEDUPE_WINDOW_SECONDS = "300"
$env:BRIDGE_HOST = "127.0.0.1"
$env:BRIDGE_PORT = "8080"
python dashboard.py
```

Webhook request example:

```python
import hashlib, hmac, json, urllib.request

secret = b"replace-with-a-long-random-secret"
body = json.dumps({"type": "invoice.paid", "data": {"id": "inv_42"}}, separators=(",", ":")).encode()
signature = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
request = urllib.request.Request(
    "http://127.0.0.1:8080/webhook",
    data=body,
    method="POST",
    headers={"Content-Type": "application/json", "X-Event-ID": "evt_42", "X-Hub-Signature-256": signature},
)
print(urllib.request.urlopen(request).status)
```

## Tests

Run the standard-library unit tests:

```powershell
python -m unittest
```

The suite covers successful verification and dispatch, tampered signatures, duplicate suppression, and exhaustion of all retry attempts into the DLQ.

## HTTP interface

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Dashboard |
| `/health` | GET | Liveness response |
| `/api/state` | GET | Counters, recent events, outage state, and DLQ snapshot |
| `/api/outage` | POST | Toggle simulated downstream outage (`{"enabled": true}`) |
| `/webhook` | POST | Signed JSON webhook; requires `X-Event-ID` and `X-Hub-Signature-256` |

Webhook results use 202 for successful dispatch, 200 for an authenticated duplicate, 401 for invalid HMAC, 400 for malformed request data, and 503 when dispatch exhausts retries.
