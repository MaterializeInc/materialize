# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: enabling storage introspection logging must not degrade
# mz_catalog_server freshness.
#
# With `enable_storage_introspection_logs` on, the storage worker in every
# clusterd (including the one backing mz_catalog_server) forwards its timely log
# events into compute's logging dataflow, where they feed the introspection
# relations mz_catalog_server indexes. The storage worker's main loop parks up to
# `statistics_collection_interval` (default 10s). Without the park-duration clamp
# in `run_client` (src/storage/src/storage_state.rs), the forwarded log stream's
# frontier would only advance on that ~10s cadence, dragging mz_catalog_server
# freshness from ~1s to ~10s. This test fails if that happens.

$ set-sql-timeout duration=120s

# `enable_storage_introspection_logs` is enabled via a system-parameter default
# at environmentd startup (see the mzcompose workflow), so it is already active
# on the mz_catalog_server replica spawned at boot. It is a feature flag that is
# not readable via SHOW, and its effect is what this test guards: reverting the
# park-duration clamp in run_client makes the freshness assertion below fail.

# A little catalog churn so mz_catalog_server processes updates while storage
# introspection events flow into its logging dataflow.
> CREATE TABLE freshness_probe_t (a int)

> CREATE VIEW freshness_probe_v AS SELECT count(*) FROM freshness_probe_t

> CREATE DEFAULT INDEX ON freshness_probe_v

> INSERT INTO freshness_probe_t VALUES (1), (2), (3)

# Sanity: mz_catalog_server index objects are present in the wallclock lag
# history, so the freshness assertion below is not vacuous (a max() over zero
# rows would be NULL, and `NULL < interval` is NULL, not true).
> SELECT count(*) > 0
  FROM mz_internal.mz_wallclock_lag_history h
  JOIN mz_catalog.mz_indexes i ON i.id = h.object_id
  JOIN mz_catalog.mz_clusters c ON c.id = i.cluster_id
  WHERE c.name = 'mz_catalog_server'
true

# Freshness guard. Over the recent window, the worst-case lag of the indexes
# served by mz_catalog_server must stay well under the 10s storage statistics
# collection cadence. We assert the *max* over a sliding window rather than the
# instantaneous lag: testdrive retries until the row matches, so an instantaneous
# check would pass on a momentary dip even while lag oscillates 0..10s. If storage
# introspection dragged the catalog server onto the 10s cadence, max(lag) sits
# near 10s and this never becomes true, failing at the timeout above.
> SELECT max(h.lag) < interval '5 seconds'
  FROM mz_internal.mz_wallclock_lag_history h
  JOIN mz_catalog.mz_indexes i ON i.id = h.object_id
  JOIN mz_catalog.mz_clusters c ON c.id = i.cluster_id
  WHERE c.name = 'mz_catalog_server'
    AND h.occurred_at + interval '30 seconds' > mz_now()
true
