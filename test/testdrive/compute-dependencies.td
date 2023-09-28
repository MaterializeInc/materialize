# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test reporting of compute dependencies through
# `mz_internal.mz_compute_dependencies`.
#
# These tests rely on testdrive's retry feature, as the
# `mz_compute_dependencies` table is asynchronously updates, so DDL commands
# are not immediately reflected there.

# There should be no compute objects yet (except for system indexes).
> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

# Test that creating compute objects causes `mz_compute_dependencies` updates.

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

> CREATE DEFAULT INDEX ON mv1

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2
mv1_primary_idx mv1

# Test that dropping compute objects causes `mz_compute_dependencies` updates.

> DROP INDEX mv1_primary_idx

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
mv1             t1
mv1             t2

> DROP MATERIALIZED VIEW mv1

> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

# Test that compute object without dependencies don't show up in
# `mz_compute_dependencies`.

> CREATE MATERIALIZED VIEW mv2 AS SELECT 1;

> SELECT * FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 'u%'

# Test that system indexes show up in `mz_compute_dependencies`.

> SELECT count(*) > 0 FROM mz_internal.mz_compute_dependencies WHERE object_id LIKE 's%'
true

# Test that `mz_compute_dependencies` is cleaned up when a cluster is dropped.

> CREATE CLUSTER cleanup SIZE '1'
> CREATE INDEX idx_cleanup IN CLUSTER cleanup ON t1 (a)
> CREATE MATERIALIZED VIEW mv_cleanup IN CLUSTER cleanup AS SELECT * FROM t2

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
idx_cleanup t1
mv_cleanup  t2

> DROP CLUSTER cleanup CASCADE

> SELECT object.name, import.name
  FROM mz_internal.mz_compute_dependencies dep
  LEFT JOIN mz_objects object ON dep.object_id = object.id
  LEFT JOIN mz_objects import ON dep.dependency_id = import.id
  WHERE object_id LIKE 'u%'
