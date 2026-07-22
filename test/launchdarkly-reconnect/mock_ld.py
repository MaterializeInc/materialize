# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
A minimal mock of the LaunchDarkly streaming API, used to exercise the SDK's
reconnect behavior (see mzcompose.py).

The first STALL_CONNECTIONS streaming clients receive an initial flag value
and then the connection goes silent: the mock holds it open without sending
any further bytes and without closing it. The SDK's transport read timeout
then fires mid-body, surfacing the exact error class of incident-984 (a
silently-dead connection producing `hyper::Error(Body, Kind(TimedOut))`). A
TCP RST or a clean FIN would not do: those are retried inside the eventsource
client on every SDK version and never reach the data source.

Every subsequent (reconnecting) client receives an updated value and periodic
heartbeats. The test asserts that the value changed, which can only happen if
the long-lived sync client reconnected after the timeout (see
STALL_CONNECTIONS for why the first stall alone is not enough).
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8080
FLAG_KEY = "reconnect-test"

# Number variations served before and after the forced reconnect. The SDK maps
# these onto the `max_result_size` system parameter (2 GiB and 3 GiB).
INITIAL_VALUE = 2147483648
RECONNECT_VALUE = 3221225472

# How many streaming connections receive the initial value and then stall.
# environmentd creates TWO LaunchDarkly clients at boot: a short-lived
# bootstrap loader (load_remote_system_parameters, dropped right after
# initialization) and the long-lived sync client. Stalling only the first
# connection would waste the stall on the bootstrap loader and hand the sync
# client the updated value with no reconnect exercised at all. Stalling the
# first two makes 3 GiB unreachable without a working post-timeout reconnect:
# the two stalls are consumed either by the sync client (which must then
# reconnect to see 3 GiB) or by a bootstrap client that itself had to reconnect
# past a stall to consume the second one. Either path exercises the behavior
# under test, so a regressed SDK stays stuck at 2 GiB.
STALL_CONNECTIONS = 2

# Seconds to hold a stalled connection open. Must comfortably exceed the SDK's
# read timeout (MZ_LAUNCHDARKLY_READ_TIMEOUT in mzcompose.py) so the timeout,
# not a connection close, ends the stream.
STALL_SECONDS = 600.0

# Seconds between heartbeats on healthy (reconnected) streams. Must be well
# below the SDK's read timeout so a healthy stream never trips it.
HEARTBEAT_SECONDS = 1.0

_lock = threading.Lock()
_stream_connections = 0


def put_event(value: int, version: int) -> bytes:
    """Serialize a LaunchDarkly streaming `put` event carrying a single flag
    that, with targeting off, evaluates to `value`."""
    flag = {
        "key": FLAG_KEY,
        "version": version,
        "on": False,
        "targets": [],
        "rules": [],
        "prerequisites": [],
        "fallthrough": {"variation": 0},
        "offVariation": 0,
        "variations": [value],
        "salt": "reconnect-test-salt",
        "clientSideAvailability": {
            "usingMobileKey": False,
            "usingEnvironmentId": False,
        },
    }
    payload = {"path": "/", "data": {"flags": {FLAG_KEY: flag}, "segments": {}}}
    return f"event: put\ndata: {json.dumps(payload)}\n\n".encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("mock-ld: " + (format % args) + "\n")

    def _send_stream_headers(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if self.path != "/all":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        global _stream_connections
        with _lock:
            _stream_connections += 1
            n = _stream_connections

        self._send_stream_headers()

        if n <= STALL_CONNECTIONS:
            # Send the initial value, then go silent while holding the
            # connection open. No further bytes, no FIN, no RST: the SDK's
            # read timeout must be what ends the stream, surfacing the
            # incident-984 error class to the data source.
            try:
                self.wfile.write(put_event(INITIAL_VALUE, 1))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                return
            self.log_message("stalling streaming connection %d", n)
            time.sleep(STALL_SECONDS)
            self.close_connection = True
            return

        # Reconnecting client: send the updated value, then hold the connection
        # open with periodic heartbeats so the SDK stays connected.
        self.wfile.write(put_event(RECONNECT_VALUE, 2))
        self.wfile.flush()
        try:
            while True:
                time.sleep(HEARTBEAT_SECONDS)
                self.wfile.write(b":heartbeat\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            return

    def do_POST(self) -> None:
        # The event processor POSTs analytics events to `/bulk`; accept and
        # discard them so it doesn't log errors.
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    sys.stderr.write(f"mock-ld: listening on {PORT}\n")
    server.serve_forever()


if __name__ == "__main__":
    main()
