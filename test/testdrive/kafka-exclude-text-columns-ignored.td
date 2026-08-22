# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The EXCLUDE COLUMNS and TEXT COLUMNS options on CREATE TABLE ... FROM SOURCE
# are meaningful only for PostgreSQL, MySQL, and SQL Server sources. For Kafka
# sources they are parsed and accepted, but silently ignored: no column is
# excluded and no column is retyped to text. This test pins that behavior so a
# future change that starts honoring (or rejecting) the options for Kafka is
# caught here.

$ set-arg-default single-replica-cluster=quickstart

$ set schema={
    "type": "record",
    "name": "test",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "item", "type": "string"},
        {"name": "qty", "type": "int"}
    ]
  }

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=exclude-text

$ kafka-ingest format=avro topic=exclude-text schema=${schema}
{"id": 1, "item": "apple", "qty": 10}
{"id": 2, "item": "banana", "qty": 20}

> CREATE SOURCE kafka_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-exclude-text-${testdrive.seed}');

# Baseline: a table without column options. Avro long -> bigint, string -> text,
# int -> integer.
> CREATE TABLE t_control FROM SOURCE kafka_src (REFERENCE "testdrive-exclude-text-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE;

> SELECT DISTINCT pg_typeof(id), pg_typeof(item), pg_typeof(qty) FROM t_control
bigint text integer

#
# EXCLUDE COLUMNS is accepted but ignored: the "excluded" columns are still
# present on the table.
#

> CREATE TABLE t_exclude FROM SOURCE kafka_src (REFERENCE "testdrive-exclude-text-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (EXCLUDE COLUMNS = (item, qty));

# If EXCLUDE COLUMNS were honored, item and qty would not exist and this query
# would error. It succeeds, proving the option had no effect.
> SELECT id, item, qty FROM t_exclude ORDER BY id
1 apple 10
2 banana 20

#
# TEXT COLUMNS is accepted but ignored: the targeted columns keep their original
# types rather than being decoded as text.
#

> CREATE TABLE t_text FROM SOURCE kafka_src (REFERENCE "testdrive-exclude-text-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (TEXT COLUMNS = (id, qty));

> SELECT DISTINCT pg_typeof(id), pg_typeof(qty) FROM t_text
bigint integer

# A numeric aggregation over a "TEXT COLUMNS" column only type-checks because the
# column is still integer, not text.
> SELECT sum(qty) FROM t_text
30

#
# Both options together are likewise accepted and ignored.
#

> CREATE TABLE t_both FROM SOURCE kafka_src (REFERENCE "testdrive-exclude-text-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (EXCLUDE COLUMNS = (item), TEXT COLUMNS = (id));

> SELECT id, item, qty, pg_typeof(id) FROM t_both ORDER BY id
1 apple 10 bigint
2 banana 20 bigint

> DROP SOURCE kafka_src CASCADE;
