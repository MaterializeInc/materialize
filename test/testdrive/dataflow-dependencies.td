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
