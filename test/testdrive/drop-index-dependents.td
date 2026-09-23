# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# No drop leaves an orphaned dataflow behind: a compute collection whose object
# is gone from the catalog, or one that reads from such a collection. The
# introspection data and the catalog update asynchronously, so each check
# relies on testdrive retrying until the count settles.
#
# test/sqllogictest/drop_index_dependents.slt covers the error messages.

$ set orphaned-dataflows
SELECT count(*) FROM mz_internal.mz_compute_dependencies WHERE (object_id LIKE 'u%' AND object_id NOT IN (SELECT global_id FROM mz_internal.mz_object_global_ids)) OR (dependency_id LIKE 'u%' AND dependency_id NOT IN (SELECT global_id FROM mz_internal.mz_object_global_ids))

> CREATE TABLE t (a int, b int)
> INSERT INTO t VALUES (1, 10), (2, 20)

# A materialized view that reads from the index blocks the drop until it is
# dropped by hand.
> CREATE INDEX t_idx ON t (a)
> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t WHERE a > 0

> SELECT count(*) FROM mz_internal.mz_compute_dependencies cd JOIN mz_internal.mz_object_global_ids g ON g.global_id = cd.dependency_id JOIN mz_indexes i ON i.id = g.id WHERE i.name = 't_idx'
1

! DROP INDEX t_idx
contains:cannot drop index "t_idx": still depended upon by materialized view "mv"

! DROP INDEX t_idx CASCADE
contains:cannot drop index "t_idx": still depended upon by materialized view "mv"

> ${orphaned-dataflows}
0

> DROP MATERIALIZED VIEW mv
> DROP INDEX t_idx

> ${orphaned-dataflows}
0

# Sibling indexes are dropped newest first.
> CREATE INDEX t_idx ON t (a)
> CREATE INDEX t_idx2 ON t (b)

! DROP INDEX t_idx
contains:cannot drop index "t_idx": still depended upon by index "t_idx2"

> DROP INDEX t_idx2
> DROP INDEX t_idx

> ${orphaned-dataflows}
0

# CASCADE drops of the relation, its schema, or its cluster take the readers
# along.
> CREATE CLUSTER drop_idx_c SIZE 'scale=1,workers=1'
> CREATE SCHEMA drop_idx_s
> CREATE TABLE drop_idx_s.t (a int)
> CREATE INDEX drop_idx_s_t_idx IN CLUSTER drop_idx_c ON drop_idx_s.t (a)
> CREATE MATERIALIZED VIEW drop_idx_s.mv IN CLUSTER drop_idx_c AS SELECT a FROM drop_idx_s.t WHERE a > 0
> DROP SCHEMA drop_idx_s CASCADE

> ${orphaned-dataflows}
0

> CREATE INDEX c_idx IN CLUSTER drop_idx_c ON t (a)
> CREATE MATERIALIZED VIEW c_mv IN CLUSTER drop_idx_c AS SELECT a FROM t WHERE a > 0
> DROP CLUSTER drop_idx_c CASCADE

> ${orphaned-dataflows}
0

> CREATE TABLE t2 (a int)
> CREATE INDEX t2_idx ON t2 (a)
> CREATE MATERIALIZED VIEW mv2 AS SELECT a FROM t2 WHERE a > 0
> DROP TABLE t2 CASCADE

> ${orphaned-dataflows}
0

# Positive control: enable_unsafe_drop_index orphans the index, which the check
# above detects, and dropping the reader frees it.
> CREATE INDEX t_idx ON t (a)
> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t WHERE a > 0

> SELECT count(*) FROM mz_internal.mz_compute_dependencies cd JOIN mz_internal.mz_object_global_ids g ON g.global_id = cd.dependency_id JOIN mz_indexes i ON i.id = g.id WHERE i.name = 't_idx'
1

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_unsafe_drop_index = true

> DROP INDEX t_idx

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enable_unsafe_drop_index

> ${orphaned-dataflows}
2

> DROP MATERIALIZED VIEW mv

> ${orphaned-dataflows}
0

> DROP TABLE t
