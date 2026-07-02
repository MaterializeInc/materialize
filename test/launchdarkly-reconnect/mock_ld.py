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

The first streaming client receives an initial flag value and then has its
connection reset mid-stream -- a non-Eof transport error, reproducing the
failure mode of incident-984. Every subsequent (reconnecting) client receives
an updated value. The test therefore asserts that the value changed, which can
only happen if the data source reconnected after the reset.
"""

import json
import socket
import struct
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

# Seconds to hold the first connection open before resetting it, leaving time
# for the SDK to apply the initial value.
HOLD_BEFORE_RESET = 3.0

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

    def _reset_connection(self) -> None:
        # Force a TCP RST instead of a clean FIN, so the SDK sees a non-Eof
        # transport error (as in incident-984) rather than the Eof it has
        # always recovered from.
        self.connection.setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
        )
        self.connection.close()

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

        if n == 1:
            # First client: send the initial value, then reset mid-stream.
            self.wfile.write(put_event(INITIAL_VALUE, 1))
            self.wfile.flush()
            time.sleep(HOLD_BEFORE_RESET)
            self.log_message("resetting first streaming connection")
            self._reset_connection()
            self.close_connection = True
            return

        # Reconnecting client: send the updated value, then hold the connection
        # open with periodic heartbeats so the SDK stays connected.
        self.wfile.write(put_event(RECONNECT_VALUE, 2))
        self.wfile.flush()
        try:
            while True:
                time.sleep(5)
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
