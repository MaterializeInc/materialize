# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from materialize.mzcompose import DEFAULT_SYSTEM_PARAMETERS


def handler_factory(config: dict[str, str]) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/polling/sdk/latest-all":
                self.handle_latest_all()
            else:
                self.handle_empty(404)

        def do_POST(self):
            self.handle_empty(200)  # accept all

        def handle_latest_all(self) -> None:
            response = json.dumps(
                {
                    "segments": {},
                    "flags": {
                        key: {
                            "key": key,
                            "on": False,
                            "prerequisites": [],
                            "targets": [],
                            "rules": [],
                            "fallthrough": {"variation": 0},
                            "offVariation": 0,
                            "variations": [val],
                            "salt": "",
                        }
                        for key, val in config.items()
                    },
                },
                indent=2,
            )
            print(str(len(response)))
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.send_header("accept-ranges", "none")
            self.end_headers()
            self.wfile.write(response.encode("utf-8"))
            self.wfile.flush()

        def handle_empty(self, code: int):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

    return Handler


def run(addr: str, port: int, config: dict[str, str]):
    server_address = (addr, port)
    httpd = HTTPServer(server_address, handler_factory(config))
    httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a simple mock LaunchDarkly server",
    )
    parser.add_argument(
        "-l",
        "--listen",
        default="0.0.0.0",
        help="Specify the IP address on which the server listens",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=9000,
        help="Specify the port on which the server listens",
    )

    args = parser.parse_args()
    run(addr=args.listen, port=args.port, config=DEFAULT_SYSTEM_PARAMETERS)
