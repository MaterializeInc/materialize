# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests `LOAD GENERATOR UPSERT`

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000
ALTER SYSTEM SET enable_load_generator_upsert = true

> CREATE CLUSTER lg_cluster SIZE '${arg.default-replica-size}'

# A loadgen that only snapshots.
> CREATE SOURCE up_no_update
  IN CLUSTER lg_cluster
  FROM LOAD GENERATOR UPSERT (
    KEYS 16,
    PARTITIONS 4,
    SNAPSHOT ROUNDS 3,
    SEED 123,
    VALUE SIZE 10,
    BATCH SIZE 2
  )

> CREATE SOURCE up_quick
  IN CLUSTER lg_cluster
  FROM LOAD GENERATOR UPSERT (
    KEYS 16,
    PARTITIONS 4,
    SNAPSHOT ROUNDS 1,
    QUICK ROUNDS 3,
    SEED 123,
    VALUE SIZE 10,
    BATCH SIZE 2
  )

# Ensure data is spread as expected.
> SELECT partition, count(*) FROM up_no_update GROUP BY partition
0   4
1   4
2   4
3   4

> SELECT partition, count(*) FROM up_quick GROUP BY partition
0   4
1   4
2   4
3   4

# 48 values produced (3 snapshot rounds with 16 keys).
# For the `QUICK ROUNDS` source, we produce a singular snapshot (16)
# + 48 values from the quick rounds. We expect 1 snapshot offset,
# and 6 quick round offsets (based on the batch size)
> SELECT
    s.name,
    u.offset_known,
    u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged,
    u.messages_received,
    u.records_indexed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('up_no_update', 'up_quick')
up_no_update 3 3 48 48 48 16
up_quick     7 7 16 16 64 16

$ set-from-sql var=pre-rehydration
SELECT
    encode(value, 'base64')
  FROM up_no_update
  WHERE
    key = 14

$ set-from-sql var=pre-rehydration-quick
SELECT
    encode(value, 'base64')
  FROM up_quick
  WHERE
    key = 14

> ALTER CLUSTER lg_cluster SET (REPLICATION FACTOR 0);

> ALTER CLUSTER lg_cluster SET (REPLICATION FACTOR 1);

# Ensure that we rehydrate and keep the same value as before.
> SELECT
    encode(value, 'base64') = '${pre-rehydration}'
  FROM up_no_update
  WHERE
    key = 14
true

> SELECT
    encode(value, 'base64') = '${pre-rehydration-quick}'
  FROM up_quick
  WHERE
    key = 14
true

> SELECT
    s.name,
    u.offset_known,
    u.offset_committed,
    u.snapshot_records_known,
    u.snapshot_records_staged,
    u.messages_received,
    u.records_indexed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('up_no_update', 'up_quick')
up_no_update 3 3 48 48 48 16
up_quick     7 7 16 16 64 16

> DROP SOURCE up_no_update
> DROP SOURCE up_quick

# Create a source with 1s updates after snapshotting.
> CREATE SOURCE up_with_update
  IN CLUSTER lg_cluster
  FROM LOAD GENERATOR UPSERT (
    KEYS 16,
    PARTITIONS 4,
    SNAPSHOT ROUNDS 3,
    SEED 123,
    VALUE SIZE 10,
    BATCH SIZE 2,
    UPDATE RATE '1s'
  )

# Ensure data is partitioned correctly.
> SELECT partition, count(*) FROM up_with_update GROUP BY partition
0   4
1   4
2   4
3   4

# Higher offsets than before, as we produce more values.
> SELECT
    s.name,
    u.offset_known > 3,
    u.offset_committed = u.offset_known,
    u.snapshot_records_known,
    u.snapshot_records_staged,
    u.messages_received > 48,
    u.records_indexed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('up_with_update')
up_with_update true true 48 48 true 16

# Also, despite the same seed, values should be different than the snapshot-only source.
> SELECT
    encode(value, 'base64') != '${pre-rehydration}'
  FROM up_with_update
  WHERE
    key = 14
true

> ALTER CLUSTER lg_cluster SET (REPLICATION FACTOR 0);

> ALTER CLUSTER lg_cluster SET (REPLICATION FACTOR 1);

$ set-from-sql var=pre-rehydration-with-update
SELECT
    encode(value, 'base64')
  FROM up_with_update
  WHERE
    key = 14

# After restarting, we should also still see new updates override values.
> SELECT
    encode(value, 'base64') != '${pre-rehydration-with-update}'
  FROM up_with_update
  WHERE
    key = 14
true
