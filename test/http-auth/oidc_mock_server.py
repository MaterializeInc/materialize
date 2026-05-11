# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ROUTES = {}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ROUTES:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(ROUTES[self.path].encode())

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    issuer, jwks_json = sys.argv[1], sys.argv[2]
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8443
    ROUTES["/.well-known/openid-configuration"] = json.dumps(
        {"issuer": issuer, "jwks_uri": f"{issuer}/.well-known/jwks.json"}
    )
    ROUTES["/.well-known/jwks.json"] = jwks_json
    print(f"OIDC mock listening on :{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
