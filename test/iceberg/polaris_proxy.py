# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import http.server
import os
import socketserver
import sys
import threading
import time
import urllib.error
import urllib.request

UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "polaris")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "8181"))

_lock = threading.Lock()
_drop_armed = False


def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    sys.stderr.write(f"[polaris-proxy {ts}] {msg}\n")
    sys.stderr.flush()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # type: ignore[override]
        return

    def _handle_control(self) -> bool:
        global _drop_armed
        if not self.path.startswith("/__control/"):
            return False
        if self.path == "/__control/drop_next_commit" and self.command == "POST":
            with _lock:
                _drop_armed = True
            self.send_response(200)
            self.send_header("Content-Length", "6")
            self.end_headers()
            self.wfile.write(b"armed\n")
            return True
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return True

    def _is_table_commit_endpoint(self) -> bool:
        parts = self.path.split("?", 1)[0].split("/")
        if "tables" not in parts:
            return False
        idx = parts.index("tables")
        return idx < len(parts) - 1 and parts[idx + 1] != ""

    def _forward(self, body: bytes | None) -> None:
        global _drop_armed
        upstream_url = f"http://{UPSTREAM_HOST}:{UPSTREAM_PORT}{self.path}"
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ("host", "transfer-encoding", "content-length")
        }
        req = urllib.request.Request(
            upstream_url, data=body, headers=headers, method=self.command
        )
        try:
            resp = urllib.request.urlopen(req)
            resp_body = resp.read()
            status = resp.status
            resp_headers = list(resp.headers.items())
        except urllib.error.HTTPError as e:
            resp_body = e.read()
            status = e.code
            resp_headers = list(e.headers.items())

        should_drop = False
        if (
            self.command == "POST"
            and self._is_table_commit_endpoint()
            and 200 <= status < 300
        ):
            with _lock:
                if _drop_armed:
                    _drop_armed = False
                    should_drop = True

        if should_drop:
            _log(f"dropping response for {self.command} {self.path}")
            self.send_response(502)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.send_response(status)
        for k, v in resp_headers:
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        if resp_body:
            self.wfile.write(resp_body)

    def _do_with_body(self) -> None:
        if self._handle_control():
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else None
        self._forward(body)

    def _do_without_body(self) -> None:
        if self._handle_control():
            return
        self._forward(None)

    def do_GET(self):
        self._do_without_body()

    def do_HEAD(self):
        self._do_without_body()

    def do_DELETE(self):
        self._do_with_body()

    def do_POST(self):
        self._do_with_body()

    def do_PUT(self):
        self._do_with_body()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    port = int(os.environ.get("PROXY_PORT", "8181"))
    server = ThreadedHTTPServer(("0.0.0.0", port), Handler)
    _log(f"listening on :{port}, upstream={UPSTREAM_HOST}:{UPSTREAM_PORT}")
    server.serve_forever()
