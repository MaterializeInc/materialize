# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Replica sizes can restrict the workload classes that their clusters run. The
# mzcompose size map defines `scale=1,workers=1,ingest-only`, which allows only
# ingest, and `scale=1,workers=1,no-ingest`, which allows everything else.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_auto_scaling_strategy = true
ALTER SYSTEM SET enable_hydration_burst = false

> SELECT size, allowed_workloads::text FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy', 'scale=1,workers=1,ingest-only', 'scale=1,workers=1,no-ingest')
"scale=1,workers=1" "{ingest,sink,compute_maintained,compute_interactive}"
"scale=1,workers=1,legacy" "{ingest,sink,compute_maintained,compute_interactive}"
"scale=1,workers=1,ingest-only" "{ingest}"
"scale=1,workers=1,no-ingest" "{sink,compute_maintained,compute_interactive}"

> CREATE CLUSTER ingest SIZE 'scale=1,workers=1,ingest-only'
> CREATE CLUSTER regular SIZE 'scale=1,workers=1'
> CREATE CLUSTER unmanaged REPLICAS (r1 (SIZE 'scale=1,workers=1'))
> CREATE CLUSTER noingest SIZE 'scale=1,workers=1,no-ingest'

> CREATE TABLE t (a int)
> INSERT INTO t VALUES (1)

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# An ingest-only cluster hosts sources and their tables, and their data is
# queryable from other clusters.

> CREATE SOURCE counter_src IN CLUSTER ingest FROM LOAD GENERATOR COUNTER
> CREATE TABLE counter FROM SOURCE counter_src

> SELECT count(*) > 0 FROM counter
true

! CREATE MATERIALIZED VIEW mv IN CLUSTER ingest AS SELECT * FROM t
contains:cluster "ingest" cannot host materialized view "mv" because its replica sizes do not allow compute_maintained workloads

! CREATE INDEX idx IN CLUSTER ingest ON t (a)
contains:cluster "ingest" cannot host index "idx" because its replica sizes do not allow compute_maintained workloads

! CREATE SINK snk
  IN CLUSTER ingest
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-workload-restrictions-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:cluster "ingest" cannot host sink "snk" because its replica sizes do not allow sink workloads

> SET cluster = ingest

! SELECT * FROM t
contains:cluster "ingest" cannot run SELECT because its replica sizes do not allow compute_interactive workloads

! SUBSCRIBE t
contains:cluster "ingest" cannot run SUBSCRIBE because its replica sizes do not allow compute_maintained workloads

! INSERT INTO t SELECT * FROM t
contains:cluster "ingest" cannot run INSERT because its replica sizes do not allow compute_interactive workloads

! INSERT INTO t VALUES (2)
contains:cluster "ingest" cannot run INSERT because its replica sizes do not allow compute_interactive workloads

! UPDATE t SET a = 2
contains:cluster "ingest" cannot run UPDATE or DELETE because its replica sizes do not allow compute_interactive workloads

# Internal users are exempt.
$ postgres-execute connection=mz_system
SET cluster = ingest
SELECT * FROM materialize.public.t

# A cluster whose sizes do not allow ingest cannot host sources.

! CREATE SOURCE counter_no IN CLUSTER noingest FROM LOAD GENERATOR COUNTER
contains:cluster "noingest" cannot host source "counter_no" because its replica sizes do not allow ingest workloads

> SET cluster = noingest

> SELECT * FROM t
1

# A size cannot be brought into a cluster whose objects it does not allow,
# whether as the managed size, a replica, or the hydration burst size.

> CREATE INDEX regular_idx IN CLUSTER regular ON t (a)

! ALTER CLUSTER regular SET (SIZE 'scale=1,workers=1,ingest-only')
contains:cannot use cluster size "scale=1,workers=1,ingest-only" on cluster "regular" because the size does not allow compute_maintained workloads, which objects on the cluster run

! ALTER CLUSTER regular SET (AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=1,ingest-only')))
contains:cannot use cluster size "scale=1,workers=1,ingest-only" on cluster "regular" because the size does not allow compute_maintained workloads, which objects on the cluster run

> CREATE INDEX unmanaged_idx IN CLUSTER unmanaged ON t (a)

! CREATE CLUSTER REPLICA unmanaged.r2 SIZE 'scale=1,workers=1,ingest-only'
contains:cannot use cluster size "scale=1,workers=1,ingest-only" on cluster "unmanaged" because the size does not allow compute_maintained workloads, which objects on the cluster run

# Objects are checked against every size the cluster may run, including its
# hydration burst size.

> CREATE CLUSTER burst (SIZE 'scale=1,workers=1', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=1,ingest-only')))

! CREATE INDEX burst_idx IN CLUSTER burst ON t (a)
contains:cluster "burst" cannot host index "burst_idx" because its replica sizes do not allow compute_maintained workloads

# A cluster hosting only sources can move between sizes.

> CREATE CLUSTER sources_only SIZE 'scale=1,workers=1'
> CREATE SOURCE counter2 IN CLUSTER sources_only FROM LOAD GENERATOR COUNTER
> ALTER CLUSTER sources_only SET (SIZE 'scale=1,workers=1,ingest-only')

# Resizing to an unrestricted size lifts the restrictions once the restricted
# replica is retired. This uses an empty cluster, which retires its old replica
# promptly. Clusters hosting a source were observed to keep the old replica for
# minutes.

> CREATE CLUSTER empty SIZE 'scale=1,workers=1,ingest-only'

> ALTER CLUSTER empty SET (SIZE 'scale=1,workers=1')

> SET cluster = empty

> SELECT * FROM t
1

> CREATE MATERIALIZED VIEW mv IN CLUSTER empty AS SELECT * FROM t

# `cluster_family_allowed_workloads` restricts every size of a family.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET cluster_family_allowed_workloads = '{"legacy": ["ingest"]}'

> SELECT size, allowed_workloads::text FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy')
"scale=1,workers=1" "{ingest,sink,compute_maintained,compute_interactive}"
"scale=1,workers=1,legacy" "{ingest}"

> CREATE CLUSTER legacy SIZE 'scale=1,workers=1,legacy'

> CREATE SOURCE counter3 IN CLUSTER legacy FROM LOAD GENERATOR COUNTER

! CREATE MATERIALIZED VIEW mv_legacy IN CLUSTER legacy AS SELECT * FROM t
contains:cluster "legacy" cannot host materialized view "mv_legacy" because its replica sizes do not allow compute_maintained workloads

> SET cluster = legacy

! SELECT * FROM t
contains:cluster "legacy" cannot run SELECT because its replica sizes do not allow compute_interactive workloads

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET cluster_family_allowed_workloads

> SELECT size, allowed_workloads::text FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy')
"scale=1,workers=1" "{ingest,sink,compute_maintained,compute_interactive}"
"scale=1,workers=1,legacy" "{ingest,sink,compute_maintained,compute_interactive}"

> SELECT * FROM t
1

> SET cluster = quickstart
