# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Index arrangement record counts reaching the join planner.
#
# Testdrive rather than sqllogictest because the counts arrive asynchronously:
# compute logs them, a per-replica introspection subscribe carries them to the
# coordinator, and only then can the optimizer read them. Testdrive retries a
# query until it matches, which is what makes that wait deterministic.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_cardinality_estimates = true
ALTER SYSTEM SET enable_index_cardinality_estimates = true

> SET enable_session_cardinality_estimates = true

> CREATE TABLE t (x int NOT NULL)

> INSERT INTO t SELECT generate_series(1, 1000)

# A view has no persist shard, so any estimate for it can only come from its
# index. This is the coverage persist structurally cannot provide.
> CREATE VIEW v AS SELECT x / 100 AS g, count(*) AS c FROM t GROUP BY x / 100

> CREATE INDEX v_g ON v (g)

> CREATE TABLE big (g int NOT NULL)

> INSERT INTO big SELECT generate_series(1, 5000)

> CREATE INDEX big_g ON big (g)

# The index holds one row per group, and `x / 100` over 1..1000 yields 0..10.
# Retried until the arrangement is built and its size has been logged.
> SELECT count(*) FROM v
11

# The export is mapped to the operator that actually holds its arrangement.
# For an index over a reduce this operator belongs to the view's part of the
# dataflow, not the index's, which is why this mapping has to come from compute.
> SELECT count(*) > 0
  FROM mz_introspection.mz_compute_export_arrangements ea
  JOIN mz_catalog.mz_objects o ON o.id = ea.export_id
  WHERE o.name = 'v_g'
true

# The planner orders the smaller input first and annotates both with the
# cardinalities it used. The 11 can only have come from the index map.
> EXPLAIN OPTIMIZED PLAN WITH(join implementations, humanized expressions) AS VERBOSE TEXT FOR
  SELECT * FROM v, big WHERE v.g = big.g
"Explained Query:\n  Project (#0{g}, #1{c}, #0{g})\n    Join on=(#0{g} = #2{g}) type=differential\n      implementation\n        %0:v[#0{g}]UKA|11| » %1:big[#0{g}]KA|5000|\n      ArrangeBy keys=[[#0{g}]]\n        ReadIndex on=v v_g=[differential join]\n      ArrangeBy keys=[[#0{g}]]\n        ReadIndex on=big big_g=[differential join]\n\nUsed Indexes:\n  - materialize.public.v_g (differential join)\n  - materialize.public.big_g (differential join)\n\nTarget cluster: quickstart\n"

# An emptied index reports zero rather than dropping out. Zero and unknown are
# very different inputs to a join orderer.
> CREATE TABLE empty_t (x int NOT NULL)

> CREATE INDEX empty_t_x ON empty_t (x)

> SELECT count(*) FROM empty_t
0
