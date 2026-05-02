# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for database-issues#11231.
#
# Test that wallclock lag is reported as NULL for introspection collections on
# a replica that has never hydrated.

$ set-sql-timeout duration=60s

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true
ALTER SYSTEM SET wallclock_lag_recording_interval = '1s'

# Create a cluster with a managed replica and wait for introspection indexes
# to hydrate.

> CREATE CLUSTER test SIZE 'scale=1,workers=1';

> WITH latest_lags AS (
    SELECT DISTINCT ON (l.object_id) l.object_id, l.lag
      FROM mz_internal.mz_wallclock_lag_history l
      JOIN mz_cluster_replicas r ON r.id = l.replica_id
      JOIN mz_clusters c ON c.id = r.cluster_id
      WHERE c.name = 'test'
      ORDER BY l.object_id, l.occurred_at DESC
    )
  SELECT DISTINCT lag < '10 seconds'
  FROM latest_lags
true

# Create an unmanaged replica pointing to non-existent endpoints, ensuring
# the controller can never connect.

> ALTER CLUSTER test SET (MANAGED = false)
> CREATE CLUSTER REPLICA test.dead_replica
    STORAGECTL ADDRESSES ['does-not-exist:2100'],
    STORAGE ADDRESSES ['does-not-exist:2103'],
    COMPUTECTL ADDRESSES ['does-not-exist:2101'],
    COMPUTE ADDRESSES ['does-not-exist:2102'],
    WORKERS 1

> WITH latest_lags AS (
    SELECT DISTINCT ON (l.object_id) l.object_id, l.lag
      FROM mz_internal.mz_wallclock_lag_history l
      JOIN mz_cluster_replicas r ON r.id = l.replica_id
      JOIN mz_clusters c ON c.id = r.cluster_id
      WHERE c.name = 'test' AND r.name = 'dead_replica'
      ORDER BY l.object_id, l.occurred_at DESC
    )
  SELECT DISTINCT lag IS NULL
  FROM latest_lags
true
