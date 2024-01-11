# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/2276.
#
# Verifies that a deep stack of dependencies can be properly created.

> CREATE MATERIALIZED VIEW test1 WITH (RETAIN HISTORY FOR '30s') AS SELECT 1

> CREATE MATERIALIZED VIEW test2 WITH (RETAIN HISTORY FOR '30s') AS SELECT * FROM test1

> CREATE MATERIALIZED VIEW test3 WITH (RETAIN HISTORY FOR '30s') AS SELECT * FROM test2

> CREATE MATERIALIZED VIEW test4 WITH (RETAIN HISTORY FOR '30s') AS SELECT * FROM test3

> CREATE MATERIALIZED VIEW test5 WITH (RETAIN HISTORY FOR '30s') AS SELECT * FROM test4

> SELECT * FROM test1
1

> SELECT * FROM test2
1

> SELECT * FROM test3
1

> SELECT * FROM test4
1

> SELECT * FROM test5
1
