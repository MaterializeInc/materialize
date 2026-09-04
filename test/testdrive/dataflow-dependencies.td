# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test reporting of dataflow dependencies through
# `mz_internal.mz_compute_dependencies` and
# `mz_internal.mz_materialization_dependencies`.
#
# These tests rely on testdrive's retry feature, as the dataflow dependency
# relations are asynchronously updated, so DDL commands are not immediately
# reflected there.

# There should be no materializations yet (except for system indexes).
> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

> SELECT * FROM mz_internal.mz_materialization_dependencies WHERE object_id LIKE 'u%'

# Test that creating objects causes dependency updates.

> CREATE TABLE t1 (a int)
> CREATE TABLE t2 (b int)

> CREATE VIEW v1 AS SELECT * FROM t1, t2
> CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM v1

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1 t1
mv1 t2

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1 t1
mv1 t2

> CREATE DEFAULT INDEX ON mv1

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}')
> CREATE SINK snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mv1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1
snk             mv1

# Test that dropping objects causes dependency updates.

> DROP SINK snk;

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

> DROP INDEX mv1_primary_idx

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2

> DROP MATERIALIZED VIEW mv1

> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

> SELECT * FROM mz_internal.mz_materialization_dependencies WHERE object_id LIKE 'u%'

# Test that objects without dependencies don't show up in the
# dependency relations.

> CREATE MATERIALIZED VIEW mv2 AS SELECT 1;

> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

> SELECT * FROM mz_internal.mz_materialization_dependencies WHERE object_id LIKE 'u%'

# Test that system indexes show up in the dependency relations.

> SELECT count(*) > 0 FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 's%'
true

> SELECT count(*) > 0 FROM mz_internal.mz_materialization_dependencies WHERE object_id LIKE 's%'
true

# Test that the dependency relations are cleaned up when a cluster is dropped.

> CREATE CLUSTER cleanup SIZE 'scale=1,workers=1'
> CREATE INDEX idx_cleanup IN CLUSTER cleanup ON t1 (a)
> CREATE MATERIALIZED VIEW mv_cleanup IN CLUSTER cleanup AS SELECT * FROM t2
> CREATE SINK snk_cleanup IN CLUSTER cleanup FROM t1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
idx_cleanup t1
mv_cleanup  t2

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
idx_cleanup t1
mv_cleanup  t2
snk_cleanup t1

> DROP CLUSTER cleanup CASCADE

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'

> SELECT object.name, import.name
  FROM mz_internal.mz_materialization_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'

# Test `mz_internal.mz_object_graph_edges`, the console workflow-graph edge set:
# the dependencies between maintained objects, plus the edges that stitch a
# source to the subsources/tables carrying its data. Like the relations above it
# is updated asynchronously, so these rely on testdrive's retry feature.

> CREATE TABLE base (a int)
> CREATE VIEW unmaintained AS SELECT * FROM base
> CREATE MATERIALIZED VIEW mvg AS SELECT * FROM unmaintained
> CREATE DEFAULT INDEX ON mvg
> CREATE SINK snk_graph
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mvg
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-graph-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# The intermediate view is not a maintained object, so it carries no edge: the
# chain is index -> mvg -> base, and sink -> mvg alongside it.
> SELECT object.name, dependency.name
  FROM mz_internal.mz_object_graph_edges e
  JOIN mz_objects object ON e.object_id = object.id
  JOIN mz_objects dependency ON e.dependency_id = dependency.id
  WHERE object.name IN ('mvg', 'mvg_primary_idx', 'snk_graph')
  ORDER BY object.name, dependency.name
mvg             base
mvg_primary_idx mvg
snk_graph       mvg

> CREATE SOURCE src_sub IN CLUSTER ${arg.single-replica-cluster} FROM LOAD GENERATOR AUCTION FOR ALL TABLES
> CREATE MATERIALIZED VIEW mv_bids AS SELECT count(*) FROM bids

# `FOR ALL TABLES` creates subsources, which are stitched to their parent source
# (bids -> src_sub), an edge that mz_materialization_dependencies does not carry,
# alongside the maintained dependency mv_bids -> bids.
> SELECT object.name, dependency.name
  FROM mz_internal.mz_object_graph_edges e
  JOIN mz_objects object ON e.object_id = object.id
  JOIN mz_objects dependency ON e.dependency_id = dependency.id
  WHERE object.name IN ('bids', 'mv_bids')
  ORDER BY object.name, dependency.name
bids    src_sub
mv_bids bids

> CREATE SOURCE src_tbl IN CLUSTER ${arg.single-replica-cluster} FROM LOAD GENERATOR AUCTION (UP TO 100)
> CREATE TABLE bids_tbl FROM SOURCE src_tbl (REFERENCE bids)

# A table fed by a source is stitched to it through `mz_tables.source_id`.
> SELECT object.name, dependency.name
  FROM mz_internal.mz_object_graph_edges e
  JOIN mz_objects object ON e.object_id = object.id
  JOIN mz_objects dependency ON e.dependency_id = dependency.id
  WHERE object.name = 'bids_tbl'
bids_tbl src_tbl

> DROP SOURCE src_sub CASCADE
> DROP SOURCE src_tbl CASCADE
> DROP TABLE base CASCADE
