# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Check that a long-running cross join does not prevent other queries
# from making progress.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mz_join_core = true

> CREATE CLUSTER linear_join REPLICAS (r1 (SIZE '1'))

> SET cluster=linear_join

> CREATE TABLE t1 (a int);

> INSERT INTO t1 SELECT generate_series(1, 100000);

> CREATE MATERIALIZED VIEW v1 IN CLUSTER linear_join AS
  SELECT SUM(a1.a + a2.a * 10000) FROM t1 AS a1, t1 AS a2;

> CREATE DEFAULT INDEX ON v1;

# Will time out unless the join above is properly using fueling to yield
> SELECT COUNT(*) > 0 FROM mz_internal.mz_dataflow_operators;
true

> SELECT COUNT(*) > 0 FROM mz_internal.mz_dataflow_operators;
true

> SELECT COUNT(*) > 0 FROM mz_internal.mz_dataflow_operators;
true

> DROP CLUSTER linear_join CASCADE;
