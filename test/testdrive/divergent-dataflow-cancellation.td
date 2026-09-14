# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test checks whether divergent WMR dataflows are correctly dropped after
# they have been cancelled by the user.
#
# We check whether the dataflow was dropped by inspecting the introspection
# sources. This also serves to verify that logging is correctly cleaned up under
# active dataflow cancellation.

# Storage operator IDs are offset by STORAGE_ID_OFFSET (1 << 48).
# Filter them out so test assertions only see compute operators.
$ set storage-id-offset=281474976710656

# Introspection subscribes add noise to the introspection sources, so disable them.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_introspection_subscribes = false

# Curated metric sinks install one dataflow per definition on every replica. CI
# runs with the feature on, so the assertions at the end filter them out instead
# of disabling it and leaving the shipping path untested. The default cluster's
# replica has run the same set since boot, so read the count from there rather
# than hardcoding it.
$ set-from-sql var=default-replica
SELECT r.name
FROM mz_catalog.mz_cluster_replicas r
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE c.name = current_setting('cluster')
ORDER BY r.name
LIMIT 1

> SET cluster_replica = '${default-replica}'

> SELECT count(*) > 0 FROM mz_introspection.mz_dataflows
  WHERE name LIKE '%metric-sink-%'
true

$ set-from-sql var=metric-sink-dataflows
SELECT count(*)::text FROM mz_introspection.mz_dataflows WHERE name LIKE '%metric-sink-%'

> RESET cluster_replica

> CREATE CLUSTER test SIZE 'scale=1,workers=1';
> SET cluster = test;

# Sink install is async; wait for all of them before capturing the baselines
# below, or one landing later looks like a cancelled dataflow that never cleaned up.
> SELECT count(*) = ${metric-sink-dataflows} FROM mz_introspection.mz_dataflows
  WHERE name LIKE '%metric-sink-%'
true

# Capture the sinks' footprint now, while they are the only dataflows here, to
# subtract from the expect-empty assertions at the end. Export ids have no useful
# order, so list them.
$ set-from-sql var=baseline-exports
SELECT string_agg(DISTINCT '''' || export_id || '''', ',')
FROM mz_introspection.mz_compute_exports_per_worker
WHERE export_id NOT LIKE 'si%'

# Operator and channel ids share one monotonic Timely counter, so a single
# high-water mark separates the sinks from what this test installs next; channels
# show up in mz_dataflow_addresses_per_worker too, so it covers both. Exclude
# storage ids, or the mark exceeds STORAGE_ID_OFFSET and the assertions go vacuous.
#
# NOTE: qualify the ORDER BY column so it sorts the numeric id, not the ::text
# projection (which ranks 999 above 1000). Keep the query fast-path, or it
# allocates ids above the mark it reports.
$ set-from-sql var=baseline-max-id
SELECT id::text AS max_id
FROM mz_introspection.mz_dataflow_addresses_per_worker
WHERE id < ${storage-id-offset}
ORDER BY mz_dataflow_addresses_per_worker.id DESC
LIMIT 1

> CREATE VIEW divergent AS
  WITH MUTUALLY RECURSIVE
      flip(x int) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip)
  SELECT * FROM flip
> CREATE INDEX divergent_index ON divergent (x)

> CREATE MATERIALIZED VIEW divergent_materialized AS
  WITH MUTUALLY RECURSIVE
      flip(x int) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip)
  SELECT * FROM flip

# Ensure the dataflow was successfully installed.
> SELECT count(*)
  FROM mz_introspection.mz_dataflows
  WHERE name LIKE '%divergent%'
2

# The dataflows just installed have to sit above the mark, or the assertions at
# the end filter away everything they are meant to catch.
> SELECT count(*) > 0
  FROM mz_introspection.mz_dataflow_operators_per_worker
  WHERE id > ${baseline-max-id} AND id < ${storage-id-offset}
true

# Drop the installed dataflows

> DROP INDEX divergent_index

> DROP MATERIALIZED VIEW divergent_materialized

# Force a statement timeout

> CREATE TABLE divergent_insert_select (f1 INTEGER);

> SET statement_timeout = '5s'

! INSERT INTO divergent_insert_select
  WITH MUTUALLY RECURSIVE flip(x int) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip)
  SELECT * FROM flip;
contains: canceling statement due to statement timeout

# Force a cursor to close
> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (
    WITH MUTUALLY RECURSIVE flip(x int) AS (VALUES(1) EXCEPT ALL SELECT * FROM flip)
    SELECT * FROM flip
  )

> FETCH ALL c WITH (timeout = '2s');

> COMMIT

# Confirm that all dataflows are now cancelled. The curated metric sinks are
# still running, so each query excludes their baseline rather than expecting a
# bare empty replica: an id above the high-water mark, or an export outside the
# captured list, belongs to a dataflow this test installed and cancelled.
#
# NOTE: It is important that the below queries are all fast-path queries.
# Otherwise they'd introduce new dataflows, confusing the results.
# For `_raw` relations, add a limit to protect against huge results.

> SELECT * FROM mz_introspection.mz_arrangement_batches_raw WHERE operator_id > ${baseline-max-id} LIMIT 10
> SELECT * FROM mz_introspection.mz_arrangement_records_raw WHERE operator_id > ${baseline-max-id} LIMIT 10
> SELECT * FROM mz_introspection.mz_arrangement_sharing_raw WHERE operator_id > ${baseline-max-id} LIMIT 10
> SELECT * FROM mz_introspection.mz_compute_error_counts_raw WHERE export_id NOT IN (${baseline-exports}) LIMIT 10
> SELECT *
  FROM mz_introspection.mz_compute_exports_per_worker
  WHERE export_id NOT LIKE 'si%' AND export_id NOT IN (${baseline-exports})
> SELECT *
  FROM mz_introspection.mz_compute_frontiers_per_worker
  WHERE export_id NOT LIKE 'si%' AND export_id NOT IN (${baseline-exports})
> SELECT *
  FROM mz_introspection.mz_compute_import_frontiers_per_worker
  WHERE export_id NOT LIKE 'si%' AND export_id NOT IN (${baseline-exports})
> SELECT * FROM mz_introspection.mz_compute_operator_durations_histogram_raw WHERE id > ${baseline-max-id} AND id < ${storage-id-offset} LIMIT 10
> SELECT id, worker_id, address::text FROM mz_introspection.mz_dataflow_addresses_per_worker WHERE id > ${baseline-max-id} AND id < ${storage-id-offset}
> SELECT * FROM mz_introspection.mz_dataflow_channels_per_worker WHERE id > ${baseline-max-id} AND id < ${storage-id-offset}
> SELECT * FROM mz_introspection.mz_dataflow_operator_reachability_raw WHERE id > ${baseline-max-id} AND id < ${storage-id-offset} LIMIT 10
> SELECT * FROM mz_introspection.mz_dataflow_operators_per_worker WHERE id > ${baseline-max-id} AND id < ${storage-id-offset}
> SELECT * FROM mz_introspection.mz_message_counts_received_raw WHERE channel_id > ${baseline-max-id} AND channel_id < ${storage-id-offset} LIMIT 10
> SELECT * FROM mz_introspection.mz_message_counts_sent_raw WHERE channel_id > ${baseline-max-id} AND channel_id < ${storage-id-offset} LIMIT 10
> SELECT * FROM mz_introspection.mz_scheduling_elapsed_raw WHERE id > ${baseline-max-id} AND id < ${storage-id-offset} LIMIT 10

# Cleanup.
> DROP CLUSTER test CASCADE
