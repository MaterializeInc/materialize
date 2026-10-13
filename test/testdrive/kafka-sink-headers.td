# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

# Test the HEADER option for Kafka sinks, which allows attaching user-specified
# headers to each Kafka message emitted by the sink.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_kafka_sink_headers = true

> CREATE CONNECTION k
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

# Test with a nonexistent column.

> CREATE TABLE wrong_name_tbl (k int);

! CREATE SINK snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM wrong_name_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-bad-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h
  FORMAT JSON ENVELOPE UPSERT
contains:HEADERS column (h) is unknown

# Test with wrong types.

> CREATE TABLE wrong_type_tbl (k int, h1 int, h2 map[text => int]);

! CREATE SINK snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM wrong_type_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-bad-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h1
  FORMAT JSON ENVELOPE UPSERT
contains:HEADERS column must have type map[text => text] or map[text => bytea]

! CREATE SINK snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM wrong_type_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-bad-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h2
  FORMAT JSON ENVELOPE UPSERT
contains:HEADERS column must have type map[text => text] or map[text => bytea]

# Test successful use with `map[text => text]`.

> CREATE TABLE text_tbl (k int, h map[text => text])
> INSERT INTO text_tbl VALUES
    (1, NULL),
    (2, '{}'),
    (3, '{"a" => null}'),
    (4, '{"a" => "b"}'),
    (5, '{"a" => "b", "c" => "d"}')

> CREATE SINK text_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM text_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-text-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h
  FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data headers=a,c format=json sink=materialize.public.text_snk key=false sort-messages=true
<missing> <missing> {"k": 1, "h": null}
<missing> <missing> {"k": 2, "h": {}}
<null>    <missing> {"k": 3, "h": {"a": null}}
b         <missing> {"k": 4, "h": {"a": "b"}}
b         d         {"k": 5, "h": {"a": "b", "c": "d"}}

> INSERT INTO text_tbl VALUES (6, '{"a" => "b", "c" => null}')

$ kafka-verify-data headers=a,c format=json sink=materialize.public.text_snk key=true sort-messages=true
b         <null>    {"k": 6} {"k": 6, "h": {"a": "b", "c": null}}

> DELETE FROM text_tbl WHERE k = 6

$ kafka-verify-data headers=a,c format=json sink=materialize.public.text_snk key=true sort-messages=true
b         <null>    {"k": 6}

# Test successful use with `map[text => bytea]`.

> CREATE TABLE bytea_tbl (k int, h map[text => bytea])
> INSERT INTO bytea_tbl VALUES
    (1, NULL),
    (2, '{}'),
    (3, '{"a" => null}'),
    (4, '{"a" => "b"}'),
    (5, '{"a" => "b", "c" => "d"}')

> CREATE SINK bytea_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM bytea_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-bytea-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h
  FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data headers=a,c format=json sink=materialize.public.bytea_snk key=false sort-messages=true
<missing> <missing> {"k": 1, "h": null}
<missing> <missing> {"k": 2, "h": {}}
<null>    <missing> {"k": 3, "h": {"a": null}}
b         <missing> {"k": 4, "h": {"a": [98]}}
b         d         {"k": 5, "h": {"a": [98], "c": [100]}}

> INSERT INTO bytea_tbl VALUES (6, '{"a" => "b", "c" => null}')

$ kafka-verify-data headers=a,c format=json sink=materialize.public.bytea_snk key=false sort-messages=true
b         <null>    {"k": 6, "h": {"a": [98], "c": null}}

# Test the `materialize-sink-id` header. The key is reserved, so a
# user-specified value is always dropped.

> CREATE TABLE sink_id_tbl (k int, h map[text => text])
> INSERT INTO sink_id_tbl VALUES
    (1, NULL),
    (2, '{"materialize-sink-id" => "custom"}')

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_sink_emit_sink_id_header = false

> CREATE SINK sink_id_off_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM sink_id_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-sink-id-off-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h
  FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data headers=materialize-sink-id format=json sink=materialize.public.sink_id_off_snk key=false sort-messages=true
<missing> {"k": 1, "h": null}
<missing> {"k": 2, "h": {"materialize-sink-id": "custom"}}

# Once enabled, the sink attaches its own id.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_sink_emit_sink_id_header = true

> CREATE SINK sink_id_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM sink_id_tbl
  INTO KAFKA CONNECTION k (TOPIC 'testdrive-sink-id-${testdrive.seed}')
  KEY (k) NOT ENFORCED
  HEADERS h
  FORMAT JSON ENVELOPE UPSERT

$ set-from-sql var=sink-id
SELECT id FROM mz_sinks WHERE name = 'sink_id_snk'

# A leading `\` keeps the `$` of the substituted row from being read as a
# testdrive command.
$ kafka-verify-data headers=materialize-sink-id format=json sink=materialize.public.sink_id_snk key=false sort-messages=true
\${sink-id} {"k": 1, "h": null}
\${sink-id} {"k": 2, "h": {"materialize-sink-id": "custom"}}
