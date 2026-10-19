# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A cluster with a source-only replica size may host sources, but cannot run
# queries or host any other object.

> SELECT size, source_only FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy', 'scale=1,workers=1,source-only')
"scale=1,workers=1" false
"scale=1,workers=1,legacy" false
"scale=1,workers=1,source-only" true

> CREATE CLUSTER so SIZE 'scale=1,workers=1,source-only'
> CREATE CLUSTER regular SIZE 'scale=1,workers=1'
> CREATE CLUSTER unmanaged REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> CREATE TABLE t (a int)
> INSERT INTO t VALUES (1)

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE counter IN CLUSTER so FROM LOAD GENERATOR COUNTER

! CREATE MATERIALIZED VIEW mv IN CLUSTER so AS SELECT * FROM t
contains:cluster "so" is source-only and cannot host materialized view "mv"

! CREATE INDEX idx IN CLUSTER so ON t (a)
contains:cluster "so" is source-only and cannot host index "idx"

! CREATE SINK snk
  IN CLUSTER so
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source-only-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:cluster "so" is source-only and cannot host sink "snk"

# Queries fail, writes that read nothing do not.

> SET cluster = so

! SELECT * FROM t
contains:cluster "so" is source-only and cannot run queries

! SUBSCRIBE t
contains:cluster "so" is source-only and cannot run queries

! INSERT INTO t SELECT * FROM t
contains:cluster "so" is source-only and cannot run queries

! UPDATE t SET a = 2
contains:cluster "so" is source-only and cannot run queries

> INSERT INTO t VALUES (2)

# Internal users are exempt.
$ postgres-execute connection=mz_system
SET cluster = so
SELECT * FROM materialize.public.t

# A source-only size cannot be introduced to a cluster hosting other objects.

> CREATE INDEX regular_idx IN CLUSTER regular ON t (a)

! ALTER CLUSTER regular SET (SIZE 'scale=1,workers=1,source-only')
contains:cannot use source-only cluster size "scale=1,workers=1,source-only" on cluster "regular" because it hosts objects other than sources

> CREATE INDEX unmanaged_idx IN CLUSTER unmanaged ON t (a)

! CREATE CLUSTER REPLICA unmanaged.r2 SIZE 'scale=1,workers=1,source-only'
contains:cannot use source-only cluster size "scale=1,workers=1,source-only" on cluster "unmanaged" because it hosts objects other than sources

# A cluster hosting only sources can move between source-only and other sizes.

> CREATE CLUSTER sources_only SIZE 'scale=1,workers=1'
> CREATE SOURCE counter2 IN CLUSTER sources_only FROM LOAD GENERATOR COUNTER
> ALTER CLUSTER sources_only SET (SIZE 'scale=1,workers=1,source-only')

# Resizing to a size that is not source-only lifts the restrictions once the
# source-only replica is retired. This uses an empty cluster, which retires its
# old replica promptly. Clusters hosting a source were observed to keep the old
# replica for minutes.

> CREATE CLUSTER so_empty SIZE 'scale=1,workers=1,source-only'

> ALTER CLUSTER so_empty SET (SIZE 'scale=1,workers=1')

> SET cluster = so_empty

> SELECT * FROM t
1
2

> CREATE MATERIALIZED VIEW mv IN CLUSTER so_empty AS SELECT * FROM t

# Sizes in a family listed in `enforce_source_only_cluster_families` are
# source-only.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enforce_source_only_cluster_families = '["legacy"]'

> SELECT size, source_only FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy', 'scale=1,workers=1,source-only')
"scale=1,workers=1" false
"scale=1,workers=1,legacy" true
"scale=1,workers=1,source-only" true

> CREATE CLUSTER legacy SIZE 'scale=1,workers=1,legacy'

> CREATE SOURCE counter3 IN CLUSTER legacy FROM LOAD GENERATOR COUNTER

! CREATE MATERIALIZED VIEW mv_legacy IN CLUSTER legacy AS SELECT * FROM t
contains:cluster "legacy" is source-only and cannot host materialized view "mv_legacy"

> SET cluster = legacy

! SELECT * FROM t
contains:cluster "legacy" is source-only and cannot run queries

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enforce_source_only_cluster_families

> SELECT size, source_only FROM mz_cluster_replica_sizes WHERE size IN ('scale=1,workers=1', 'scale=1,workers=1,legacy', 'scale=1,workers=1,source-only')
"scale=1,workers=1" false
"scale=1,workers=1,legacy" false
"scale=1,workers=1,source-only" true

> SELECT * FROM t
1
2

> SET cluster = quickstart
