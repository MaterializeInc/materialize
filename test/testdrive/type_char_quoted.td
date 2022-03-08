# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for Postgres' "char" type. (Note quotes)

> SELECT pg_typeof('a'::"char")
"\"char\""

> SELECT 'a'::"char"
97

! SELECT 'a'::"char(5)"
contains: unknown catalog item 'char(5)'

! SELECT 'a'::"char"(5)
contains: pg_catalog.char does not support type modifiers

> SELECT 'abcd'::"char"
97

> CREATE TABLE t1 (f1 "char");

> INSERT INTO t1 VALUES ('a');

> SELECT * FROM t1;
97

# Usage in materialized views

> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1 UNION ALL SELECT 'b'::"char"

> SELECT * FROM v1;
97
98

# Usage in joins

> SELECT * FROM v1 LEFT JOIN t1 ON (t1.f1 = v1.f1)
97 97
98 <null>
