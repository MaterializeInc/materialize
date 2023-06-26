# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that with `enable_create_source_denylist_with_options` off, if
# `enable_disk_cluster_replicas` is on, we can use `WITH(DISK)`.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_source_denylist_with_options = false
ALTER SYSTEM SET enable_disk_cluster_replicas = true

> DROP CLUSTER IF EXISTS c;

> CREATE CLUSTER c REPLICAS (dff_1 (SIZE '1', DISK), dff_2 (SIZE '1'))

> SELECT name, size, disk FROM mz_catalog.mz_cluster_replicas WHERE name LIKE '%dff_%'
dff_1 1 true
dff_2 1 false

> DROP CLUSTER IF EXISTS c;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_disk_cluster_replicas = false

! CREATE CLUSTER c REPLICAS (dff_3 (size '1', disk))
contains:`WITH (DISK)` for cluster replicas is not supported

# The following test that we don't crash envd with bad parameters, and instead just fallback
# to safe parameters.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET upsert_rocksdb_universal_compaction_ratio = 50;
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET upsert_rocksdb_universal_compaction_ratio;
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET upsert_rocksdb_parallelism = -1;
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET upsert_rocksdb_parallelism
