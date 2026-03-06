# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default default-storage-size=scale=1,workers=1

# =============================================================================
# Test Debezium JSON envelope support (generic mode)
# =============================================================================

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# --- Basic CRUD operations ---

$ kafka-create-topic topic=dbz-json partitions=1

# Use simple integer keys to avoid key-terminator conflicts with JSON colons
$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json
1:{"before":null,"after":{"id":1,"creature":"mudskipper"},"op":"c"}
1:{"before":{"id":1,"creature":"mudskipper"},"after":{"id":1,"creature":"salamander"},"op":"u"}
1:{"before":{"id":1,"creature":"salamander"},"after":{"id":1,"creature":"lizard"},"op":"u"}

> CREATE CLUSTER dbz_json_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE dbz_json_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-${testdrive.seed}');

> CREATE TABLE dbz_json_tbl FROM SOURCE dbz_json_src (REFERENCE "testdrive-dbz-json-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM
> COMMIT

# Output schema is [key: jsonb, data: jsonb]
# The last update for key=1 should win (lizard)
# Use ->> to extract fields since JSON key order is non-deterministic
> SELECT key, data->>'id', data->>'creature' FROM dbz_json_tbl
1 1 lizard

# --- Insert a second row ---

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json
2:{"before":null,"after":{"id":2,"creature":"archeopteryx"},"op":"c"}
2:{"before":{"id":2,"creature":"archeopteryx"},"after":{"id":2,"creature":"velociraptor"},"op":"u"}

> SELECT key, data->>'creature' FROM dbz_json_tbl ORDER BY key
1 lizard
2 velociraptor

# --- Test delete (op=d) ---

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json
1:{"before":{"id":1,"creature":"lizard"},"after":null,"op":"d"}

> SELECT key, data->>'creature' FROM dbz_json_tbl
2 velociraptor

# --- Test reinsertion after delete ---

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json
1:{"before":null,"after":{"id":1,"creature":"chicken"},"op":"c"}

> SELECT key, data->>'creature' FROM dbz_json_tbl ORDER BY key
1 chicken
2 velociraptor

# --- Test snapshot read (op=r) ---

$ kafka-create-topic topic=dbz-json-snapshot partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json-snapshot
10:{"before":null,"after":{"id":10,"name":"snapshot_row"},"op":"r"}

> BEGIN
> CREATE SOURCE dbz_json_snap_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-snapshot-${testdrive.seed}');

> CREATE TABLE dbz_json_snap_tbl FROM SOURCE dbz_json_snap_src (REFERENCE "testdrive-dbz-json-snapshot-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT key, data->>'name' FROM dbz_json_snap_tbl
10 snapshot_row

# --- Test payload-wrapped format (Debezium default with schema+payload) ---

$ kafka-create-topic topic=dbz-json-wrapped partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json-wrapped
100:{"schema":{"type":"struct","fields":[]},"payload":{"before":null,"after":{"id":100,"val":"wrapped"},"op":"c"}}

> BEGIN
> CREATE SOURCE dbz_json_wrapped_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-wrapped-${testdrive.seed}');

> CREATE TABLE dbz_json_wrapped_tbl FROM SOURCE dbz_json_wrapped_src (REFERENCE "testdrive-dbz-json-wrapped-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT key, data->>'val' FROM dbz_json_wrapped_tbl
100 wrapped

# --- Test INCLUDE OFFSET metadata ---

$ kafka-create-topic topic=dbz-json-meta partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json-meta
1:{"before":null,"after":{"id":1,"val":"first"},"op":"c"}
2:{"before":null,"after":{"id":2,"val":"second"},"op":"c"}

> BEGIN
> CREATE SOURCE dbz_json_meta_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-meta-${testdrive.seed}');

> CREATE TABLE dbz_json_meta_tbl FROM SOURCE dbz_json_meta_src (REFERENCE "testdrive-dbz-json-meta-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  INCLUDE OFFSET
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT key, data->>'val', "offset" FROM dbz_json_meta_tbl ORDER BY key
1 first 0
2 second 1

# --- Error case: missing KEY FORMAT ---

$ kafka-create-topic topic=dbz-json-nokey partitions=1

> CREATE SOURCE dbz_json_nokey_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-nokey-${testdrive.seed}');

! CREATE TABLE dbz_json_nokey_tbl FROM SOURCE dbz_json_nokey_src (REFERENCE "testdrive-dbz-json-nokey-${testdrive.seed}")
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

# --- Error case: non-JSON/non-Avro format ---

! CREATE TABLE dbz_json_nokey_tbl FROM SOURCE dbz_json_nokey_src (REFERENCE "testdrive-dbz-json-nokey-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE DEBEZIUM
contains:ENVELOPE DEBEZIUM requires that VALUE FORMAT is set to AVRO or JSON

# --- Test INCLUDE DEBEZIUM METADATA ---

$ kafka-create-topic topic=dbz-json-dbzmeta partitions=1

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-json-dbzmeta
1:{"before":null,"after":{"id":1,"val":"hello"},"op":"c","ts_ms":1000,"source":{"connector":"test"}}

> BEGIN
> CREATE SOURCE dbz_json_dbzmeta_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-dbzmeta-${testdrive.seed}');

> CREATE TABLE dbz_json_dbzmeta_tbl FROM SOURCE dbz_json_dbzmeta_src (REFERENCE "testdrive-dbz-json-dbzmeta-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  INCLUDE DEBEZIUM METADATA AS dbz_envelope
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT key, data->>'val', dbz_envelope->>'op', dbz_envelope->>'ts_ms' FROM dbz_json_dbzmeta_tbl
1 hello c 1000

# --- Test INCLUDE DEBEZIUM METADATA with default column name ---

> BEGIN
> CREATE SOURCE dbz_json_dbzmeta_default_src
  IN CLUSTER dbz_json_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-json-dbzmeta-${testdrive.seed}');

> CREATE TABLE dbz_json_dbzmeta_default_tbl FROM SOURCE dbz_json_dbzmeta_default_src (REFERENCE "testdrive-dbz-json-dbzmeta-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  INCLUDE DEBEZIUM METADATA
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT key, debezium_metadata->>'op' FROM dbz_json_dbzmeta_default_tbl
1 c

# --- Error case: INCLUDE DEBEZIUM METADATA without ENVELOPE DEBEZIUM ---

! CREATE TABLE dbz_json_nokey_tbl FROM SOURCE dbz_json_nokey_src (REFERENCE "testdrive-dbz-json-nokey-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  INCLUDE DEBEZIUM METADATA
  ENVELOPE UPSERT
contains:INCLUDE DEBEZIUM METADATA requires ENVELOPE DEBEZIUM with VALUE FORMAT JSON
