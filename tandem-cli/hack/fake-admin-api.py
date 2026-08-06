#!/usr/bin/env python3
"""Fake Admin API for manually trying tandem-cli against something that talks the
contract, without a real tandem-admin instance running. Not part of the Go module (no
go.mod entry, not built/tested by it) - a standalone dev convenience kept in the repo.
Usage: python3 fake-admin-api.py 8080
"""
import http.server, json, sys, re, random, urllib.parse

# Freshly randomized on every request, not on a server-side timer - since --watch polls
# at its own --interval, this alone makes the dashboard visibly change every refresh.
_state = {"done": 1000}

def _random_summary():
    _state["done"] += random.randint(0, 20)  # DONE only grows, like a real outbox's would
    pending = random.randint(0, 1000)
    in_flight = random.randint(0, 1000)
    failed = random.randint(0, 1000)
    discarded = random.randint(0, 5)
    return {
        "counts": {
            "PENDING": pending,
            "IN_FLIGHT": in_flight,
            "DONE": _state["done"],
            "FAILED": failed,
            "DISCARDED": discarded,
        },
        "lagCount": pending,
        "lagAgeSeconds": round(random.uniform(0, 60), 1),
    }

# Rows 1 and 3 deliberately share a correlation id across DIFFERENT aggregates: that is the real
# shape of the relationship (one correlation id groups a whole business operation, spanning several
# aggregates), so `outbox search --correlation-id incident-corr` returns both and demonstrates it.
OUTBOX = {
    1: {"id": 1, "aggregateId": "order-1", "aggregateType": "Order", "seq": 1, "status": "DONE",
        "attempts": 1, "createdAt": "2026-08-05T09:00:00Z", "payload": {"amount": 42},
        "correlationId": "incident-corr",
        "headers": {"content-type": "application/json", "correlation-id": "incident-corr"}},
    2: {"id": 2, "aggregateId": "order-2", "aggregateType": "Order", "seq": 1, "status": "FAILED",
        "attempts": 10, "createdAt": "2026-08-05T09:05:00Z", "lastError": "broker unreachable",
        "correlationId": "other-corr", "headers": {"correlation-id": "other-corr"}},
    3: {"id": 3, "aggregateId": "shipment-9", "aggregateType": "Shipment", "seq": 1, "status": "PENDING",
        "attempts": 0, "createdAt": "2026-08-05T09:06:00Z",
        "correlationId": "incident-corr",
        "headers": {"content-type": "application/json", "correlation-id": "incident-corr"}},
}

class H(http.server.BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json" if code < 400 else "application/problem+json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        m = re.match(r"^/outbox/messages/(\d+)$", self.path)
        if self.path == "/outbox/summary":
            self._send(200, _random_summary())
        elif self.path.startswith("/outbox/messages?") or self.path == "/outbox/messages":
            # Only correlationId is honoured, so the incident-time lookup can actually be tried
            # end to end; every OTHER filter is still ignored and returns the full list.
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            correlation_id = query.get("correlationId", [None])[0]
            items = [r for r in OUTBOX.values()
                     if correlation_id is None or r.get("correlationId") == correlation_id]
            self._send(200, {"items": items, "nextCursor": None})
        elif m and int(m.group(1)) in OUTBOX:
            self._send(200, OUTBOX[int(m.group(1))])
        elif self.path == "/relay/status":
            self._send(200, {"state": "RUNNING", "bucketCount": 256, "uncoveredBuckets": 0, "workers": 2})
        elif self.path.startswith("/relay/buckets"):
            self._send(200, [{"bucket": 0, "covered": True, "paused": False, "pendingCount": 2,
                               "owner": "worker-1", "lagAgeSeconds": 3.1}])
        elif self.path == "/relay/workers":
            self._send(200, [{"workerId": "worker-1", "bucketCount": 256, "lastHeartbeat": "2026-08-05T09:10:00Z"}])
        else:
            self._send(404, {"type": "https://tandem.codingful.com/problems/not-found", "title": "Resource not found", "status": 404})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = json.loads(self.rfile.read(length)) if length else {}
        m = re.match(r"^/outbox/messages/(\d+)/(replay|discard)$", self.path)
        if m and int(m.group(1)) == 2 and m.group(2) == "discard":
            entry = dict(OUTBOX[2], status="DISCARDED", discardReason=raw.get("reason"))
            self._send(200, entry)
        elif m and int(m.group(1)) == 2 and m.group(2) == "replay":
            self._send(200, dict(OUTBOX[2], status="PENDING"))
        elif self.path == "/outbox/replay":
            dry = raw.get("dryRun", False)
            self._send(200, {"matched": 3, "replayed": 0 if dry else 3, "dryRun": dry})
        elif self.path in ("/relay/pause", "/relay/resume"):
            state = "PAUSED" if self.path == "/relay/pause" else "RUNNING"
            self._send(200, {"state": state, "bucketCount": 256, "uncoveredBuckets": 0, "workers": 2})
        elif re.match(r"^/relay/buckets/(\d+)/release$", self.path):
            bucket = int(re.match(r"^/relay/buckets/(\d+)/release$", self.path).group(1))
            self._send(200, {"bucket": bucket, "covered": True, "paused": False, "pendingCount": 0})
        else:
            self._send(404, {"type": "https://tandem.codingful.com/problems/not-found", "title": "Resource not found", "status": 404})

    def log_message(self, format, *args):
        pass

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
print(f"Fake Admin API listening on http://127.0.0.1:{port}")
http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
