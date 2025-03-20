# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

> DROP CLUSTER IF EXISTS test_cluster CASCADE;
> CREATE CLUSTER test_cluster (SIZE '1');

> CREATE SOURCE count IN CLUSTER test_cluster FROM LOAD GENERATOR COUNTER

> SELECT
    u.rehydration_latency IS NOT NULL
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('count')
true

> ALTER CLUSTER test_cluster SET (REPLICATION FACTOR = 0)
