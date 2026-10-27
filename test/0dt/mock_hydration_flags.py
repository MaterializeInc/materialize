# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Suspend hydration on waiting's replicas and on mixed.r2."""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FLAG = {
    "key": "hydration-concurrency",
    "version": 1,
    "on": True,
    "targets": [],
    "prerequisites": [],
    "rules": [
        {
            "id": "held-replica",
            "clauses": [
                {
                    "contextKind": "replica",
                    "attribute": "cluster_name",
                    "op": "in",
                    "values": ["mixed"],
                    "negate": False,
                },
                {
                    "contextKind": "replica",
                    "attribute": "replica_name",
                    "op": "in",
                    "values": ["r2"],
                    "negate": False,
                },
            ],
            "variation": 1,
            "trackEvents": False,
        },
        {
            "id": "held-cluster",
            "clauses": [
                {
                    "contextKind": "replica",
                    "attribute": "cluster_name",
                    "op": "in",
                    "values": ["waiting"],
                    "negate": False,
                }
            ],
            "variation": 1,
            "trackEvents": False,
        },
    ],
    "fallthrough": {"variation": 0},
    "offVariation": 0,
    "variations": [4, 0],
    "salt": "hydration-test",
    "clientSideAvailability": {
        "usingMobileKey": False,
        "usingEnvironmentId": False,
    },
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path != "/all":
            self.send_response(200 if self.path == "/health" else 404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        payload = {"path": "/", "data": {"flags": {FLAG["key"]: FLAG}, "segments": {}}}
        try:
            self.wfile.write(f"event: put\ndata: {json.dumps(payload)}\n\n".encode())
            self.wfile.flush()
            while True:
                time.sleep(1)
                self.wfile.write(b":heartbeat\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
