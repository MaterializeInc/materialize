# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SCHEMA other;

> CREATE TABLE a (c int)
> CREATE TABLE b (c int)
> CREATE TABLE generate_series (c int)
> CREATE TABLE other.a (c int)

> SELECT * FROM a, other.a;
c c
---

# alias avoids collision
> SELECT * FROM a, a AS b;
c c
---

# subquery avoids collision
> SELECT * FROM a, (SELECT * FROM a);
c c
---

# joined tables collide
! SELECT * FROM a, a;
contains:table name "a" specified more than once

! SELECT * FROM b, (SELECT * FROM a, a);
contains:table name "a" specified more than once

! SELECT * FROM other.a, other.a;
contains:table name "a" specified more than once

! SELECT * FROM a LEFT JOIN a ON TRUE;
contains:table name "a" specified more than once

! SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN a;
contains:table name "a" specified more than once

! SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN b;
contains:table name "b" specified more than once

! DELETE FROM a USING a;
contains:table name "a" specified more than once

# alias introduces collision
! SELECT * FROM a, b AS a;
contains:table name "a" specified more than once

! SELECT * FROM a, other.a AS a;
contains:table name "a" specified more than once

! SELECT * FROM a AS z, b AS z;
contains:table name "z" specified more than once

! SELECT * FROM a, (SELECT * FROM a) AS a;
contains:table name "a" specified more than once

# table function names
! SELECT * FROM generate_series, generate_series(1,2)
contains:table name "generate_series" specified more than once

! SELECT * FROM a AS generate_series, generate_series(1,2)
contains:table name "generate_series" specified more than once

! SELECT * FROM generate_series AS a, generate_series(1,2) AS a
contains:table name "a" specified more than once

! SELECT * FROM a, generate_series(1,2) AS a
contains:table name "a" specified more than once

# CTEs

! WITH a AS (SELECT 1) SELECT * FROM a, a;
contains:table name "a" specified more than once
