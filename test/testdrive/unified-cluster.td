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

$ set-arg-default default-replica-size=scale=1,workers=1

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unified_cluster = true

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

# Storage names ingestion dataflows by id, "Source dataflow: <id>". On a
# two-cluster replica the ingestion runs on the separate storage Timely
# cluster and is invisible here, so this assertion fails if the topology flag
# does not reach the replica.
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflows WHERE name LIKE 'Source dataflow:%'
true

# RF-only ALTER acknowledges desired configuration before policy materializes
# replicas. Observe both boundaries so this exercises a real suspend/resume.
> CREATE TABLE uc_table (a int)
> CREATE INDEX uc_table_idx IN CLUSTER uc ON uc_table (a)
> INSERT INTO uc_table VALUES (1)
> SELECT * FROM uc_table
1

> ALTER CLUSTER uc SET (REPLICATION FACTOR 0)
> SELECT count(*) FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'uc'
0

> INSERT INTO uc_table VALUES (2)
> ALTER CLUSTER uc SET (REPLICATION FACTOR 1)
> SELECT count(*) FROM mz_cluster_replicas r
  JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'uc'
1

# Membership convergence is retryable. The subsequent read must work once.
$ set-max-tries max-tries=1

> SELECT * FROM uc_table
1
2

> RESET cluster

> DROP SOURCE uc_auction CASCADE

> DROP TABLE uc_table

> DROP CLUSTER uc

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_unified_cluster
