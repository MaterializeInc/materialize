#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


"""A mock telemetry server.

This is a mock of the telemetry server that runs at telemetry.materialize.com.
It is intended for manual testing of materialized's telemetry reporting.
(Automating this test would be nice, but it is not trivial.)

The following is an example of how you might use this mock telemetry server.
First, launch the server:

    $ bin/pyactivate -m materialize.cli.mock_telemetry_server

This generates a self-signed SSL certificate named "localhost.crt" in the
current directory. You'll need to tell your system to trust this certificate.
On macOS, add the certificate to the "login" keychain and mark the certificate
as always trusted. On Linux, set the `SSL_CERT_FILE` environment variable:

    $ export SSL_CERT_FILE=localhost.crt

In another terminal, launch Materialize:

    $ cargo run -- --dev --telemetry-domain localhost:4000 --telemetry-interval 5s

Notice how we configure Materialize to target the mock telemetry server. The
SSL_CERT_FILE environment variable instructs OpenSSL to trust the self-signed
certificate the server created. Using a small telemetry reporting interval means
you won't be waiting hours to observe multiple turns of the reporting loop.

As the mock telemetry server receives requests, it logs them. You can add
sources and sinks to Materialize and verify that the data reported to the mock
telemetry server evolves accordingly.

By default, the mock telemetry server claims the latest version is the version
specified in the Cargo.toml for the materialized crate. You can update its
latest version on the fly with a PUT request:

    $ SSL_CERT_FILE=localhost.crt curl https://localhost:4000 -X PUT --data 9.9.9

Increasing the latest version like so should cause Materialize to log a "new
version" notice. You may need to check the log file to see the notice.
"""

import json
import os
import os.path
import ssl
from http.server import BaseHTTPRequestHandler, HTTPServer

from materialize import ROOT
from materialize.cargo import Workspace


class Handler(BaseHTTPRequestHandler):
    def read_body(self) -> str:
        content_length = int(self.headers["content-length"])
        return self.rfile.read(content_length).decode("UTF-8")

    def do_PUT(self) -> None:
        global latest_release
        latest_release = self.read_body()
        self.send_response(200)
        self.end_headers()

    def do_POST(self) -> None:
        print(json.loads(self.read_body()))
        return self.do_GET()

    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        body = json.dumps({"latest_release": latest_release})
        self.wfile.write(body.encode("UTF-8"))


if __name__ == "__main__":
    if os.path.exists("localhost.crt"):
        print("localhost.crt already exists, not regenerating")
    else:
        print("Generating self-signed cert for localhost...")
        os.system(
            "openssl req -nodes -x509 -newkey rsa:4096 -keyout localhost.crt -out localhost.crt -subj '/CN=localhost'"
        )

    cargo_workspace = Workspace(ROOT)
    latest_release = str(cargo_workspace.crate_for_bin("materialized").version)

    httpd = HTTPServer(("localhost", 4000), Handler)
    httpd.socket = ssl.wrap_socket(
        httpd.socket, server_side=True, certfile="localhost.crt"
    )

    print("Listening on localhost:4000...")
    httpd.serve_forever()
