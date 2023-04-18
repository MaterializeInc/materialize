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
  FROM mz_internal.mz_dataflows
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

# Confirm that all dataflows are now cancelled

> SELECT count(*) FROM mz_internal.mz_active_peeks_per_worker
0

> SELECT count(*) FROM mz_internal.mz_arrangement_batches_raw
0

> SELECT count(*) FROM mz_internal.mz_arrangement_records_raw
0

> SELECT count(*) FROM mz_internal.mz_arrangement_sharing_raw
0

> SELECT count(*) FROM mz_internal.mz_compute_delays_histogram_raw
0

> SELECT count(*) FROM mz_internal.mz_compute_dependencies_per_worker
0

> SELECT count(*) FROM mz_internal.mz_compute_exports_per_worker
0

# One frontier for each introspection arrangement.
> SELECT count(*)
  FROM mz_internal.mz_compute_frontiers_per_worker
  WHERE worker_id = 0
19

> SELECT count(*) FROM mz_internal.mz_compute_import_frontiers_per_worker
0

> SELECT count(*) FROM mz_internal.mz_compute_operator_durations_histogram_raw
0

> SELECT count(*) FROM mz_internal.mz_dataflow_addresses_per_worker
0

> SELECT count(*) FROM mz_internal.mz_dataflow_channels_per_worker
0

> SELECT count(*) FROM mz_internal.mz_dataflow_operator_reachability_raw
0

> SELECT count(*) FROM mz_internal.mz_dataflow_operators_per_worker
0

> SELECT count(*) FROM mz_internal.mz_message_counts_received_raw
0

> SELECT count(*) FROM mz_internal.mz_message_counts_sent_raw
0

# This source never sees retractions.
> SELECT count(*) > 0 FROM mz_internal.mz_peek_durations_histogram_raw
true

> SELECT count(*) FROM mz_internal.mz_scheduling_elapsed_raw
0

# This source never sees retractions.
> SELECT count(*) > 0 FROM mz_internal.mz_scheduling_parks_histogram_raw
true
