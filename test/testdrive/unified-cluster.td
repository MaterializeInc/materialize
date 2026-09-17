# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A unified cluster hosts storage objects on the compute Timely cluster: a
# source ingests there, and its dataflow is visible in compute introspection.
# The topology is baked in at replica provisioning, so the parameter must be
# set before the cluster is created.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unified_cluster = true

# Disable the storage log bridge, which the CI configuration turns on. The
# bridge would make storage dataflows visible in compute introspection on a
# two-cluster replica as well, so with it enabled the dataflow assertion below
# holds on either topology and the file carries no signal that the flag
# reached the replica.
$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_storage_introspection_logs = false

> CREATE CLUSTER uc (SIZE '${arg.default-replica-size}')

> CREATE SOURCE uc_auction
  IN CLUSTER uc
  FROM LOAD GENERATOR AUCTION (TICK INTERVAL '100ms')
  FOR ALL TABLES

# The source ingests.
> SELECT count(*) > 0 FROM bids
true

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'uc_auction'
running

# The ingestion dataflow runs on the compute Timely cluster and is natively
# visible in compute introspection, without a storage log bridge.
> SET cluster = uc

# Storage names ingestion dataflows by id, "Source dataflow: <id>".
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflows WHERE name LIKE 'Source dataflow:%'
true

> SELECT count(*) FROM mz_introspection.mz_dataflow_operators WHERE name = 'Replay storage timely logs'
0

> RESET cluster

> DROP SOURCE uc_auction CASCADE

> DROP CLUSTER uc

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_storage_introspection_logs

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_unified_cluster
