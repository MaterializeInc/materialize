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
# Test Debezium JSON envelope support with TiCDC mode
# TiCDC uses the same Debezium format but may include extra fields like
# commit_ts, cluster_id, etc.
# =============================================================================

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# --- Basic TiCDC CRUD operations ---

$ kafka-create-topic topic=dbz-ticdc partitions=1

# TiCDC messages include additional TiDB-specific fields
# Use simple integer keys to avoid key-terminator conflicts with JSON colons
$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-ticdc
1:{"before":null,"after":{"id":1,"name":"alice"},"op":"c","source":{"version":"1.0","connector":"tidb","name":"test","commit_ts":1000},"ts_ms":1609459200000}
2:{"before":null,"after":{"id":2,"name":"bob"},"op":"c","source":{"version":"1.0","connector":"tidb","name":"test","commit_ts":1001},"ts_ms":1609459200001}
1:{"before":{"id":1,"name":"alice"},"after":{"id":1,"name":"alice_updated"},"op":"u","source":{"version":"1.0","connector":"tidb","name":"test","commit_ts":1002},"ts_ms":1609459200002}

> CREATE CLUSTER dbz_ticdc_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE dbz_ticdc_src
  IN CLUSTER dbz_ticdc_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-ticdc-${testdrive.seed}');

> CREATE TABLE dbz_ticdc_tbl FROM SOURCE dbz_ticdc_src (REFERENCE "testdrive-dbz-ticdc-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  INCLUDE DEBEZIUM METADATA AS dbz_meta
  ENVELOPE DEBEZIUM (MODE = 'TICDC')
> COMMIT

> SELECT key, data->>'name' FROM dbz_ticdc_tbl ORDER BY key
1 alice_updated
2 bob

# --- Verify INCLUDE DEBEZIUM METADATA preserves CDC metadata ---
# The dbz_meta column contains the full Debezium envelope including source metadata.
# This is critical for TiCDC users who need commit_ts to correlate with Dumpling snapshots.
> SELECT key, dbz_meta->'source'->>'commit_ts' FROM dbz_ticdc_tbl ORDER BY key
1 1002
2 1001

# ts_ms is also available
> SELECT key, dbz_meta->>'ts_ms' FROM dbz_ticdc_tbl ORDER BY key
1 1609459200002
2 1609459200001

# --- Test delete ---

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-ticdc
2:{"before":{"id":2,"name":"bob"},"after":null,"op":"d","source":{"version":"1.0","connector":"tidb","name":"test","commit_ts":1003},"ts_ms":1609459200003}

> SELECT key, data->>'name' FROM dbz_ticdc_tbl
1 alice_updated

# --- Test reinsertion ---

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=dbz-ticdc
2:{"before":null,"after":{"id":2,"name":"bob_v2"},"op":"c","source":{"version":"1.0","connector":"tidb","name":"test","commit_ts":1004},"ts_ms":1609459200004}

> SELECT key, data->>'name' FROM dbz_ticdc_tbl ORDER BY key
1 alice_updated
2 bob_v2

# --- Verify MODE syntax parsing ---

# MODE parameter is case-insensitive
> BEGIN
> CREATE SOURCE dbz_ticdc_upper_src
  IN CLUSTER dbz_ticdc_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-ticdc-${testdrive.seed}');

> CREATE TABLE dbz_ticdc_upper_tbl FROM SOURCE dbz_ticdc_upper_src (REFERENCE "testdrive-dbz-ticdc-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM (MODE = 'ticdc')
> COMMIT

> SELECT key, data->>'name' FROM dbz_ticdc_upper_tbl ORDER BY key
1 alice_updated
2 bob_v2

# --- Error case: invalid MODE value ---

> CREATE SOURCE dbz_ticdc_bad_src
  IN CLUSTER dbz_ticdc_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-ticdc-${testdrive.seed}');

! CREATE TABLE dbz_ticdc_bad_tbl FROM SOURCE dbz_ticdc_bad_src (REFERENCE "testdrive-dbz-ticdc-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM (MODE = 'INVALID')
contains:Expected TICDC
