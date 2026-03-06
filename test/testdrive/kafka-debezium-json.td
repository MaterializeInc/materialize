# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Debezium JSON envelope support.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# =============================================================================
# Test 1: Basic insert/update/delete operations with unwrapped Debezium JSON
# =============================================================================

$ kafka-create-topic topic=dbz-json-basic partitions=1

# Insert two rows (op: "c")
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-basic
{"id":1}|{"before":null,"after":{"id":1,"creature":"fish"},"op":"c"}
{"id":2}|{"before":null,"after":{"id":2,"creature":"bird"},"op":"c"}

> CREATE SOURCE dbz_json_basic_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-basic-${testdrive.seed}');

> CREATE TABLE dbz_json_basic (id int4 NOT NULL, creature text NOT NULL, PRIMARY KEY (id))
  FROM SOURCE dbz_json_basic_src (REFERENCE "testdrive-dbz-json-basic-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

> SELECT * FROM dbz_json_basic ORDER BY id
id creature
----------
1  fish
2  bird

# Update a row (op: "u")
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-basic
{"id":1}|{"before":{"id":1,"creature":"fish"},"after":{"id":1,"creature":"mudskipper"},"op":"u"}

> SELECT * FROM dbz_json_basic ORDER BY id
id creature
----------
1  mudskipper
2  bird

# Delete a row (op: "d")
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-basic
{"id":2}|{"before":{"id":2,"creature":"bird"},"after":null,"op":"d"}

> SELECT * FROM dbz_json_basic ORDER BY id
id creature
----------
1  mudskipper

# =============================================================================
# Test 2: Wrapped form (schema+payload) and snapshot operation (op: "r")
# =============================================================================

$ kafka-create-topic topic=dbz-json-wrapped partitions=1

# Insert using wrapped form with "schema" and "payload" keys
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-wrapped
{"id":10}|{"schema":{},"payload":{"before":null,"after":{"id":10,"name":"Alice"},"op":"r"}}
{"id":20}|{"schema":{},"payload":{"before":null,"after":{"id":20,"name":"Bob"},"op":"c"}}

> CREATE SOURCE dbz_json_wrapped_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-wrapped-${testdrive.seed}');

> CREATE TABLE dbz_json_wrapped (id int4 NOT NULL, name text NOT NULL, PRIMARY KEY (id))
  FROM SOURCE dbz_json_wrapped_src (REFERENCE "testdrive-dbz-json-wrapped-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

> SELECT * FROM dbz_json_wrapped ORDER BY id
id name
-------
10 Alice
20 Bob

# Tombstone message (null value with key) treated as delete
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-wrapped
{"id":10}|

> SELECT * FROM dbz_json_wrapped ORDER BY id
id name
-------
20 Bob

# =============================================================================
# Test 3: Multiple data types
# =============================================================================

$ kafka-create-topic topic=dbz-json-types partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-types
{"id":1}|{"before":null,"after":{"id":1,"small_int":42,"big_int":9876543210,"real_num":3.14,"double_num":2.718281828,"is_active":true,"name":"test","data":{"nested":"value"}},"op":"c"}

> CREATE SOURCE dbz_json_types_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-types-${testdrive.seed}');

> CREATE TABLE dbz_json_types (
    id int4 NOT NULL,
    small_int int2 NOT NULL,
    big_int int8 NOT NULL,
    real_num float4 NOT NULL,
    double_num float8 NOT NULL,
    is_active boolean NOT NULL,
    name text NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (id)
  )
  FROM SOURCE dbz_json_types_src (REFERENCE "testdrive-dbz-json-types-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

> SELECT id, small_int, big_int, is_active, name FROM dbz_json_types
id small_int big_int     is_active name
----------------------------------------------
1  42        9876543210  true      test

> SELECT real_num::text, double_num::text FROM dbz_json_types
real_num double_num
-------------------
3.14     2.718281828

> SELECT data->>'nested' as nested FROM dbz_json_types
nested
------
value

# =============================================================================
# Test 4: Standard Debezium connector format with extra metadata fields
# =============================================================================

$ kafka-create-topic topic=dbz-json-metadata partitions=1

# Full Debezium message with source metadata, ts_ms, transaction etc.
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-metadata
{"id":1}|{"before":null,"after":{"id":1,"val":"hello"},"op":"c","ts_ms":1234567890,"source":{"connector":"mysql","db":"mydb","table":"mytable","file":"binlog.000001","pos":100,"row":0,"snapshot":"false"},"transaction":{"id":"txn-001","total_order":1}}

> CREATE SOURCE dbz_json_metadata_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-metadata-${testdrive.seed}');

> CREATE TABLE dbz_json_metadata (id int4 NOT NULL, val text NOT NULL, PRIMARY KEY (id))
  FROM SOURCE dbz_json_metadata_src (REFERENCE "testdrive-dbz-json-metadata-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

# Extra metadata fields (source, ts_ms, transaction) should be silently ignored
> SELECT * FROM dbz_json_metadata
id val
-------
1  hello

# =============================================================================
# Test 5: Missing "before" field (absent, not null) on insert
# =============================================================================

$ kafka-create-topic topic=dbz-json-no-before partitions=1

# Insert where "before" key is entirely absent
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-no-before
{"id":1}|{"after":{"id":1,"val":"present"},"op":"c"}

> CREATE SOURCE dbz_json_no_before_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-no-before-${testdrive.seed}');

> CREATE TABLE dbz_json_no_before (id int4 NOT NULL, val text NOT NULL, PRIMARY KEY (id))
  FROM SOURCE dbz_json_no_before_src (REFERENCE "testdrive-dbz-json-no-before-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

> SELECT * FROM dbz_json_no_before
id val
-------
1  present

# =============================================================================
# Test 6: Error: missing KEY FORMAT with ENVELOPE DEBEZIUM
# =============================================================================

$ kafka-create-topic topic=dbz-json-no-key partitions=1

> CREATE SOURCE dbz_json_no_key_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-no-key-${testdrive.seed}');

! CREATE TABLE dbz_json_no_key (id int4 NOT NULL, val text NOT NULL, PRIMARY KEY (id))
  FROM SOURCE dbz_json_no_key_src (REFERENCE "testdrive-dbz-json-no-key-${testdrive.seed}")
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

# =============================================================================
# Test 7: Composite primary key
# =============================================================================

$ kafka-create-topic topic=dbz-json-composite-key partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-composite-key
{"org_id":1,"user_id":10}|{"before":null,"after":{"org_id":1,"user_id":10,"name":"Alice"},"op":"c"}
{"org_id":1,"user_id":20}|{"before":null,"after":{"org_id":1,"user_id":20,"name":"Bob"},"op":"c"}
{"org_id":2,"user_id":10}|{"before":null,"after":{"org_id":2,"user_id":10,"name":"Charlie"},"op":"c"}

> CREATE SOURCE dbz_json_composite_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-composite-key-${testdrive.seed}');

> CREATE TABLE dbz_json_composite (org_id int4 NOT NULL, user_id int4 NOT NULL, name text NOT NULL, PRIMARY KEY (org_id, user_id))
  FROM SOURCE dbz_json_composite_src (REFERENCE "testdrive-dbz-json-composite-key-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM

> SELECT * FROM dbz_json_composite ORDER BY org_id, user_id
org_id user_id name
-----------------------
1      10      Alice
1      20      Bob
2      10      Charlie

# Update one row with composite key
$ kafka-ingest format=bytes key-format=bytes key-terminator=| topic=dbz-json-composite-key
{"org_id":1,"user_id":10}|{"before":{"org_id":1,"user_id":10,"name":"Alice"},"after":{"org_id":1,"user_id":10,"name":"Alicia"},"op":"u"}

> SELECT * FROM dbz_json_composite ORDER BY org_id, user_id
org_id user_id name
-----------------------
1      10      Alicia
1      20      Bob
2      10      Charlie

# =============================================================================
# Test 8: Error: no column definitions with JSON + ENVELOPE DEBEZIUM
# =============================================================================

$ kafka-create-topic topic=dbz-json-no-cols partitions=1

> CREATE SOURCE dbz_json_no_cols_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-no-cols-${testdrive.seed}');

! CREATE TABLE dbz_json_no_cols
  FROM SOURCE dbz_json_no_cols_src (REFERENCE "testdrive-dbz-json-no-cols-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM
contains:ENVELOPE DEBEZIUM with FORMAT JSON requires explicit column definitions
