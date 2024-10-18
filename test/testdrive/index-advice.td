# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CLUSTER c1 REPLICAS (r1 (SIZE '1'));
> CREATE CLUSTER c2 REPLICAS (r1 (SIZE '1'));

> CREATE MATERIALIZED VIEW v11 IN CLUSTER c1 AS (SELECT 1);

> CREATE VIEW v21 AS (SELECT * FROM v11);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v21;

> CREATE VIEW v31 AS (SELECT * FROM v21);
> CREATE VIEW v32 AS (SELECT * FROM v21);
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v32;

> CREATE VIEW v41 AS (SELECT * FROM v31);
> CREATE DEFAULT INDEX IN CLUSTER c2 ON v41;
> CREATE DEFAULT INDEX IN CLUSTER c1 ON v41;

> CREATE MATERIALIZED VIEW v42 IN CLUSTER c1 AS (SELECT * FROM v31 UNION ALL SELECT * FROM v32);
> CREATE DEFAULT INDEX IN CLUSTER c2 ON v42;


> SELECT * FROM mz_internal.mz_index_advice;
1
