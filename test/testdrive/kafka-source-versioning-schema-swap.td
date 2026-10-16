# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Kafka sources do not support ALTER SOURCE ... ADD/DROP SUBSOURCE, so the way
# to absorb an upstream (topic) schema change is the blue/green pattern: stand
# up a parallel set of tables/views that read the evolved schema in a staging
# schema, then atomically ALTER SCHEMA ... SWAP WITH the production schema.
# Consumers keep referencing the stable production names and transparently pick
# up the new schema after the swap.

$ set-arg-default single-replica-cluster=quickstart

# The value schema a producer starts with.
$ set schema-v1={
    "type": "record",
    "name": "order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "item", "type": "string"}
    ]
  }

# A backward-compatible evolution: a new field with a default, so old records
# still decode under the new reader schema.
$ set schema-v2={
    "type": "record",
    "name": "order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "item", "type": "string"},
        {"name": "quantity", "type": "int", "default": 0}
    ]
  }

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=orders

$ kafka-ingest format=avro topic=orders schema=${schema-v1}
{"id": 1, "item": "apples"}
{"id": 2, "item": "bananas"}

#
# Production environment reading the initial schema.
#

> CREATE SCHEMA prod;

> CREATE SOURCE orders_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-orders-${testdrive.seed}');

# A table created now pins the reader schema present in the registry at creation
# time, i.e. schema-v1.
> CREATE TABLE prod.orders FROM SOURCE orders_src (REFERENCE "testdrive-orders-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE;

# A downstream consumer that references the production name.
> CREATE VIEW prod.orders_by_item AS
  SELECT item, count(*) AS orders FROM prod.orders GROUP BY item;

> SELECT id, item FROM prod.orders ORDER BY id
1 apples
2 bananas

# The new field does not exist yet on the production table.
! SELECT quantity FROM prod.orders
contains:column "quantity" does not exist

> SELECT item, orders FROM prod.orders_by_item ORDER BY item
apples 1
bananas 1

#
# Upstream schema change: the producer starts emitting the evolved schema. This
# registers a new version under the same subject.
#

$ kafka-ingest format=avro topic=orders schema=${schema-v2}
{"id": 3, "item": "cherries", "quantity": 100}

# The production table keeps its pinned v1 reader schema. It ingests the new
# record but resolves away the added field, so `quantity` is still absent.
> SELECT id, item FROM prod.orders ORDER BY id
1 apples
2 bananas
3 cherries

! SELECT quantity FROM prod.orders
contains:column "quantity" does not exist

#
# Staging environment reading the evolved schema.
#

> CREATE SCHEMA staging;

# Created after the evolution, so this table pins the v2 reader schema and
# exposes `quantity`. Old records get the schema default (0).
> CREATE TABLE staging.orders FROM SOURCE orders_src (REFERENCE "testdrive-orders-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE;

# The staging consumer takes advantage of the new field.
> CREATE VIEW staging.orders_by_item AS
  SELECT item, sum(quantity) AS total_quantity FROM staging.orders GROUP BY item;

> SELECT id, item, quantity FROM staging.orders ORDER BY id
1 apples 0
2 bananas 0
3 cherries 100

#
# Cut over: atomically swap staging into production. Consumers referencing the
# `prod` names now see the evolved schema without changing their references.
#

> BEGIN;
> ALTER SCHEMA prod SWAP WITH staging;
> COMMIT;

# prod.orders is now the table pinned to the v2 reader schema.
> SELECT id, item, quantity FROM prod.orders ORDER BY id
1 apples 0
2 bananas 0
3 cherries 100

> SELECT item, total_quantity FROM prod.orders_by_item ORDER BY item
apples 0
bananas 0
cherries 100

# The old v1 table now lives in the staging schema and still lacks `quantity`.
! SELECT quantity FROM staging.orders
contains:column "quantity" does not exist

> SELECT id, item FROM staging.orders ORDER BY id
1 apples
2 bananas
3 cherries

# Both tables keep reading the same underlying source.
> SELECT count(*) FROM mz_tables WHERE name = 'orders';
2

# Once the swap is validated, the old environment can be dropped.
> DROP SCHEMA staging CASCADE;

> SELECT id, item, quantity FROM prod.orders ORDER BY id
1 apples 0
2 bananas 0
3 cherries 100

> DROP SOURCE orders_src CASCADE;
> DROP SCHEMA prod CASCADE;
