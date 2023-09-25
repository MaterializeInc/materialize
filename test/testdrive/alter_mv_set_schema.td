# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Addiitonal tests for ALTER MATERIALIZED VIEW SET SCHEMA, to complement those
# in sqllogictest/alter.slt
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_set_cluster = true

# Check that moving the schema does not count towards the max_materialized_views limit

> CREATE TABLE t1 (f1 INTEGER);
> CREATE CLUSTER alter_mv_set_cluster REPLICAS (replica1 (SIZE '1'));
> CREATE MATERIALIZED VIEW mv1 AS SELECT f1 + 1 AS f1 FROM t1;
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET max_materialized_views=1
> ALTER MATERIALIZED VIEW mv1 SET CLUSTER alter_mv_set_cluster;
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET max_materialized_views

# The cluster that contains the moved view should not be droppable
! DROP CLUSTER alter_mv_set_cluster
contains: cannot drop cluster with active objects

# And should not allow sources to be created
! CREATE SOURCE lgc IN CLUSTER alter_mv_set_cluster FROM LOAD GENERATOR COUNTER;
contains: cannot create source in cluster containing indexes or materialized views

# Sinks should survive a MOVE

> CREATE TABLE sink_table (f1 INTEGER);

> INSERT INTO sink_table VALUES (1);

> CREATE MATERIALIZED VIEW sink_view AS SELECT f1 + 1 AS f1 FROM sink_table;

> CREATE DEFAULT INDEX ON sink_view;

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SINK sink_sink
  FROM sink_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'alter-mv-set-schema-sink-topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE SOURCE sink_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'alter-mv-set-schema-sink-topic')
  FORMAT BYTES
  ENVELOPE NONE

> SELECT COUNT(*) FROM sink_source;
1

> ALTER MATERIALIZED VIEW sink_view SET CLUSTER alter_mv_set_cluster;

> INSERT INTO sink_table VALUES (2);

> SELECT COUNT(*) FROM sink_source;
2

# TODO(#21956): Remove once fixed
> DROP SOURCE sink_source;
> DROP SINK sink_sink;
> DROP MATERIALIZED VIEW mv1, sink_view;

# Cleanup
# TODO(#21956): Add CASCADE once fixed and explicit drop removed
> DROP CLUSTER alter_mv_set_cluster;
