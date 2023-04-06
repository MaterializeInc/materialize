# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# This REST call will cause toxiproxy to close the connection after processing 64K bytes

$ http-request method=POST url=http://toxiproxy:8474/proxies/postgres/toxics content-type=application/json
{
  "name": "postgres",
  "type": "timeout",
  "timeout": 0
}

> SELECT mz_internal.mz_sleep(5);
<null>
