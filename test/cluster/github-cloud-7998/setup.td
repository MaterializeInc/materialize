# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;

# Create a cluster that we can make unavailable.
> CREATE CLUSTER compute REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      STORAGE ADDRESSES ['clusterd1:2102'],
      COMPUTE ADDRESSES ['clusterd1:2103'],
      WORKERS 1
    )
  )

# Create a serving cluster that should still respond to queries.
> CREATE CLUSTER serving SIZE '1'

# Create an MV maintained on the compute cluster and indexed on the serving
# cluster. It's important that the MV reads from an index too, to reproduce
# the bug.

> CREATE TABLE source (a int)
> CREATE INDEX source_idx IN CLUSTER compute ON source (a)
> CREATE MATERIALIZED VIEW mv1 IN CLUSTER compute AS SELECT * FROM source
> CREATE INDEX mv1_idx IN CLUSTER serving ON mv1 (a)

# Create a second MV that transitively depends on several indexes, to also test
# this case.

> CREATE VIEW v1 AS SELECT a + 1 AS b FROM source;
> CREATE INDEX v1_idx IN CLUSTER compute ON v1 (b);
> CREATE VIEW v2 AS SELECT b + 1 AS c FROM v1;
> CREATE INDEX v2_idx IN CLUSTER compute ON v2 (c);
> CREATE VIEW v3 AS SELECT c + 1 AS d FROM v2;
> CREATE INDEX v3_idx IN CLUSTER compute ON v3 (d);
> CREATE MATERIALIZED VIEW mv2 IN CLUSTER compute AS SELECT * FROM v3
> CREATE INDEX mv2_idx IN CLUSTER serving ON mv2 (d)

# ... and an MV that depends on multiple indexes directly.

> CREATE VIEW v4 AS SELECT a + 1 AS e FROM source;
> CREATE INDEX v4_idx IN CLUSTER compute ON v4 (e);
> CREATE VIEW v5 AS SELECT a + 1 AS f FROM source;
> CREATE INDEX v5_idx IN CLUSTER compute ON v5 (f);
> CREATE MATERIALIZED VIEW mv3 IN CLUSTER compute AS SELECT e + f AS g FROM v4, v5
> CREATE INDEX mv3_idx IN CLUSTER serving ON mv3 (g)

# Let things hydrate.

> INSERT INTO source VALUES (1)
> SET cluster = serving

> SELECT * FROM mv1
1

> SELECT * FROM mv2
4

> SELECT * FROM mv3
4
