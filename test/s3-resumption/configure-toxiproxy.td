# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Set up a connection between toxiproxy:4566 and localstack:4566 which
# is the entrypoint for all localstack AWS services

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "localstack",
  "listen": "0.0.0.0:4566",
  "upstream": "localstack:4566"
}
