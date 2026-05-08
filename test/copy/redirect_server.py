#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import socket
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

CONTAINER_IP = socket.gethostbyname(socket.gethostname())
CSV_DATA = b"ssrf_bypass_confirmed\n"


class RedirectHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self._handle(include_body=False)

    def do_GET(self):
        self._handle(include_body=True)

    def _handle(self, include_body: bool) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()

        elif self.path == "/data.csv":
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(CSV_DATA)))
            self.end_headers()
            if include_body:
                self.wfile.write(CSV_DATA)

        elif self.path == "/redirect-to-docker-ip":
            self.send_response(302)
            self.send_header("Location", f"http://{CONTAINER_IP}:8080/data.csv")
            self.end_headers()

        elif self.path == "/redirect-to-loopback":
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:8080/data.csv")
            self.end_headers()

        elif self.path == "/redirect-to-metadata":
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        sys.stderr.write(
            f"[redirect-server] {self.client_address[0]} - {format % args}\n"
        )
        sys.stderr.flush()


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), RedirectHandler).serve_forever()
