# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Exercises the blue/green schema-swap technique for versioning a webhook
# source. A replacement webhook source with a different header configuration is
# built in a separate schema, then swapped into place so that consumers keep
# referencing the original name without any downtime.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_swap = true;

$ set-arg-default default-storage-size=scale=1,workers=1

> CREATE CLUSTER webhook_versioning_cluster REPLICAS (r1 (SIZE '${arg.default-storage-size}'));

> CREATE SCHEMA prod;

# v1: the webhook exposes only the `app` header, projected into its own column.
> CREATE SOURCE prod.events IN CLUSTER webhook_versioning_cluster FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'app' AS app;

> SHOW COLUMNS FROM prod.events;
name  nullable  type   comment
-------------------------------
app   true      text   ""
body  false     jsonb  ""

$ webhook-append database=materialize schema=prod name=events content-type=application/json app=orders
{"id":1}

$ webhook-append database=materialize schema=prod name=events content-type=application/json app=orders
{"id":2}

> SELECT body, app FROM prod.events;
"{\"id\":1}" orders
"{\"id\":2}" orders

# Capture the original prod schema id so we can prove the swap exchanged the
# two schemas.
$ set-from-sql var=prod-schema-id
SELECT id::text FROM mz_schemas WHERE name = 'prod';

# v2: build the replacement in a staging schema. Unlike v1, this version
# captures every header as a `headers` map instead of a single `app` column.
> CREATE SCHEMA staging;

> CREATE SOURCE staging.events IN CLUSTER webhook_versioning_cluster FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS;

> SHOW COLUMNS FROM staging.events;
name     nullable  type   comment
----------------------------------
body     false     jsonb  ""
headers  false     map    ""

$ webhook-append database=materialize schema=staging name=events content-type=application/json app=orders
{"id":100}

> SELECT body, headers -> 'app' FROM staging.events;
"{\"id\":100}" orders

# Cut over: swap the staging schema into prod. Consumers keep referencing
# `prod.events`, which now resolves to the v2 definition.
> ALTER SCHEMA prod SWAP WITH staging;

# The schema that used to be named `prod` is now named `staging`.
> SELECT name FROM mz_schemas WHERE id = '${prod-schema-id}';
staging

# `prod.events` now has the v2 shape (a `headers` map) and serves the v2 data.
> SHOW COLUMNS FROM prod.events;
name     nullable  type   comment
----------------------------------
body     false     jsonb  ""
headers  false     map    ""

> SELECT body, headers -> 'app' FROM prod.events;
"{\"id\":100}" orders

# The retired v1 definition now lives under the staging name, keeping its data.
> SHOW COLUMNS FROM staging.events;
name  nullable  type   comment
-------------------------------
app   true      text   ""
body  false     jsonb  ""

> SELECT body, app FROM staging.events;
"{\"id\":1}" orders
"{\"id\":2}" orders

# New posts to `prod.events` land in the v2 source, exposing the headers map.
$ webhook-append database=materialize schema=prod name=events content-type=application/json app=payments
{"id":200}

> SELECT body, headers -> 'app' FROM prod.events;
"{\"id\":100}" orders
"{\"id\":200}" payments

# Once the cutover is validated, drop the retired definition.
> DROP SCHEMA staging CASCADE;

> SELECT body, headers -> 'app' FROM prod.events;
"{\"id\":100}" orders
"{\"id\":200}" payments
