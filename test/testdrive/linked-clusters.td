# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_format_json = true

# The expected number of rows in system tables depends on the number of replicas
$ skip-if
SELECT ${arg.replicas} > 1;

$ set-sql-timeout duration=1s

# Create a basic view for use throughout the test.
> CREATE MATERIALIZED VIEW v AS SELECT 1 AS a

# Test that only the default replicas exist at start.
> SHOW CLUSTERS
default
mz_introspection
mz_system
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default           r1  ${arg.default-replica-size}
mz_introspection  r1  1
mz_system         r1  1

# Test that creating a source without an explicit size creates a linked cluster
# with the default size. (Unsafe mode only.)
> CREATE SOURCE lg FROM LOAD GENERATOR COUNTER
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_lg
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                r1      ${arg.default-replica-size}
mz_introspection       r1      1
mz_system              r1      1
materialize_public_lg  linked  ${arg.default-storage-size}
> DROP SOURCE lg

# Test that creating sources with an explicit size creates linked clusters, each
# with a replica named "linked" with the size specified in the source.
> CREATE SOURCE lg1 FROM LOAD GENERATOR COUNTER WITH (SIZE = '1')
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_lg1
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_public_lg1  linked  1
> SELECT s.name, c.name
  FROM mz_sources s
  JOIN mz_clusters c ON c.id = s.cluster_id
  WHERE s.id LIKE 'u%'
lg1  materialize_public_lg1
> CREATE SOURCE lg2 FROM LOAD GENERATOR COUNTER WITH (SIZE = '2')
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_lg1
materialize_public_lg2
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_public_lg1  linked  1
materialize_public_lg2  linked  2

# Test altering the size of a source.
> ALTER SOURCE lg2 SET (SIZE = '4')
> SELECT size FROM (SHOW SOURCES) WHERE name = 'lg2'
4
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_lg2'
materialize_public_lg2  linked  4
> ALTER SOURCE lg2 RESET (SIZE)
> SELECT size FROM (SHOW SOURCES) WHERE name = 'lg2'
\${arg.default-storage-size}
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_lg2'
materialize_public_lg2  linked  ${arg.default-storage-size}

# Create a connection for use in sinks.
> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}')

# Test that linked clusters cannot be dropped, nor can their replicas be
# dropped, nor can new replicas be created, nor can they run queries,
# nor can new sources be scheduled on them.
! DROP CLUSTER materialize_public_lg2 CASCADE
contains:cannot modify linked cluster "materialize_public_lg2"
! DROP CLUSTER REPLICA materialize_public_lg2.linked
contains:cannot modify linked cluster "materialize_public_lg2"
! CREATE CLUSTER REPLICA materialize_public_lg2.new SIZE 'small'
contains:cannot create more than one replica of a cluster containing sources or sinks
! CREATE MATERIALIZED VIEW w IN CLUSTER materialize_public_lg2 AS SELECT 1
contains:cannot modify linked cluster "materialize_public_lg2"
! CREATE INDEX IN CLUSTER materialize_public_lg2 ON v (a)
contains:cannot modify linked cluster "materialize_public_lg2"
! CREATE SOURCE lg_bad IN CLUSTER "materialize_public_lg1" FROM LOAD GENERATOR COUNTER
contains:cannot modify linked cluster "materialize_public_lg1"
! CREATE SINK snk_bad IN CLUSTER "materialize_public_lg1" FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk-bad-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM
contains:cannot modify linked cluster "materialize_public_lg1"
> SET cluster = materialize_public_lg2
! SELECT 1
contains:cannot execute queries on cluster containing sources or sinks
! SUBSCRIBE v
contains:cannot execute queries on cluster containing sources or sinks
! EXPLAIN SELECT 1
contains:cannot execute queries on cluster containing sources or sinks
> SET cluster = default

# Test that only the default clusters and replicas remain after dropping
# the sources.
> DROP SOURCE lg1
> DROP SOURCE lg2
> SHOW CLUSTERS
default
mz_introspection
mz_system
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1

# Test that creating a sink without an explicit size creates a linked cluster
# with the default size. (Unsafe mode only.)
> CREATE SINK snk FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_snk
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_public_snk  linked  ${arg.default-storage-size}
> DROP SINK snk

# Test that creating sinks with an explicit size creates linked clusters, each
# with a replica named "linked" with the size specified in the sink.
> CREATE SINK snk1 FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk1-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM WITH (SIZE = '1')
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_snk1
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                  r1      ${arg.default-replica-size}
mz_introspection         r1      1
mz_system                r1      1
materialize_public_snk1  linked  1
> SELECT s.name, c.name
  FROM mz_sinks s
  JOIN mz_clusters c ON c.id = s.cluster_id
  WHERE s.id LIKE 'u%'
snk1  materialize_public_snk1
> CREATE SINK snk2 FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM WITH (SIZE = '2')
> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_snk1
materialize_public_snk2
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                  r1      ${arg.default-replica-size}
mz_introspection         r1      1
mz_system                r1      1
materialize_public_snk1  linked  1
materialize_public_snk2  linked  2

# Test altering the size of a sink.
> ALTER SINK snk2 SET (SIZE = '4')
> SELECT size FROM (SHOW SINKS) WHERE name = 'snk2'
4
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_snk2'
materialize_public_snk2  linked  4
> ALTER SINK snk2 RESET (SIZE)
> SELECT size FROM (SHOW SINKS) WHERE name = 'snk2'
\${arg.default-storage-size}
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS) WHERE cluster = 'materialize_public_snk2'
materialize_public_snk2  linked  ${arg.default-storage-size}

# Test that only the default clusters and replicas remain after dropping
# the sinks.
> DROP SINK snk1
> DROP SINK snk2
> SHOW CLUSTERS
default
mz_introspection
mz_system
> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1

# Test that two objects with the same name create distinct clusters
# as the schema name is included in the cluster name

> CREATE SCHEMA schema1;
> CREATE SCHEMA schema2;

> CREATE SOURCE schema1.s1 FROM LOAD GENERATOR COUNTER
> CREATE SINK schema2.s1 FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_schema1_s1
materialize_schema2_s1

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_schema1_s1  linked  ${arg.default-storage-size}
materialize_schema2_s1  linked  ${arg.default-storage-size}

> DROP SCHEMA schema1 CASCADE;
> DROP SCHEMA schema2 CASCADE;

# Test capitalizarion behavior

> CREATE SOURCE "s1" FROM LOAD GENERATOR COUNTER
> CREATE SINK "S1" FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'snk-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_s1
materialize_public_S1

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_public_s1  linked  ${arg.default-storage-size}
materialize_public_S1  linked  ${arg.default-storage-size}

> DROP SOURCE "s1";
> DROP SINK "S1";

# Test that an already-existing cluster is taken into account when
# selecting a name for the linked cluster

> CREATE CLUSTER materialize_public_s1 REPLICAS (r1 (SIZE '1'));
> CREATE SOURCE s1 FROM LOAD GENERATOR COUNTER

> SHOW CLUSTERS
default
mz_introspection
mz_system
materialize_public_s1
materialize_public_s11

> SELECT cluster, replica, size FROM (SHOW CLUSTER REPLICAS)
default                 r1      ${arg.default-replica-size}
mz_introspection        r1      1
mz_system               r1      1
materialize_public_s1  r1  1
materialize_public_s11 linked ${arg.default-storage-size}

> DROP CLUSTER materialize_public_s1;
> DROP SOURCE s1;
