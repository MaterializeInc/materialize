# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT COUNT(*) = 0 FROM mz_internal.mz_source_statuses WHERE error LIKE '%connection closed%';
true

# This REST call will cause toxiproxy to close the connection.

$ http-request method=POST url=http://toxiproxy:8474/proxies/postgres content-type=application/json
{
  "name": "postgres",
  "listen": "0.0.0.0:5432",
  "upstream": "postgres:5432",
  "enabled": false
}

> SELECT COUNT(*) > 0 FROM mz_internal.mz_source_statuses WHERE error LIKE '%connection closed%';
true

# Ensure we don't lose ceased status even if source is stalled
> SELECT name, status FROM mz_internal.mz_source_statuses;
pg_source           stalled
t                   ceased

$ http-request method=POST url=http://toxiproxy:8474/proxies/postgres content-type=application/json
{
  "name": "postgres",
  "listen": "0.0.0.0:5432",
  "upstream": "postgres:5432",
  "enabled": true
}
