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

# Introspection subscribes add noise to the introspection sources, so disable them.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_introspection_subscribes = false

> CREATE CLUSTER test SIZE 'scale=1,workers=1';
> SET cluster = test;

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

# Confirm that all dataflows are now cancelled.
#
# NOTE: It is important that the below queries are all fast-path queries.
# Otherwise they'd introduce new dataflows, confusing the results.
# For `_raw` relations, add a limit to protect against huge results.

> SELECT * FROM mz_introspection.mz_arrangement_batches_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_arrangement_records_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_arrangement_sharing_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_compute_error_counts_raw LIMIT 10
> SELECT *
  FROM mz_introspection.mz_compute_exports_per_worker
  WHERE export_id NOT LIKE 'si%'
> SELECT *
  FROM mz_introspection.mz_compute_frontiers_per_worker
  WHERE export_id NOT LIKE 'si%'
> SELECT *
  FROM mz_introspection.mz_compute_import_frontiers_per_worker
  WHERE export_id NOT LIKE 'si%'
> SELECT * FROM mz_introspection.mz_compute_operator_durations_histogram_raw LIMIT 10
> SELECT id, worker_id, address::text FROM mz_introspection.mz_dataflow_addresses_per_worker
> SELECT * FROM mz_introspection.mz_dataflow_channels_per_worker
> SELECT * FROM mz_introspection.mz_dataflow_operator_reachability_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_dataflow_operators_per_worker
> SELECT * FROM mz_introspection.mz_message_counts_received_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_message_counts_sent_raw LIMIT 10
> SELECT * FROM mz_introspection.mz_scheduling_elapsed_raw LIMIT 10

# Cleanup.
> DROP CLUSTER test CASCADE
