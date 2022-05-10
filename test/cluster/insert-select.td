# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test INSERT INTO...SELECT. This must be a testdrive test to avoid symbiosis
# in sqllogictest.

> CREATE TABLE t (i INT, f REAL, t TEXT);
> INSERT INTO t VALUES (1, 2, 'a'), (3, 4, 'b');

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b

> CREATE TABLE u (i INT, f REAL, t TEXT);
> INSERT INTO u VALUES (5, 6, 'c');

> INSERT INTO t SELECT * FROM u;

# Assignment casts are valid
> CREATE TABLE bigger (i INT8, f FLOAT, t TEXT);
> INSERT INTO bigger VALUES (7, 8, 'd');

> INSERT INTO t SELECT * FROM bigger;

# Obliquely go through SELECT * FROM ( VALUES ... )
> INSERT INTO t SELECT * FROM (
    VALUES (9.1::numeric, 10, 'e')
  );

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b
5 6 c
7 8 d
9 10 e

! INSERT INTO t SELECT * FROM (
    VALUES ('11', '12', 'f')
  );
contains:column "i" is of type integer but expression is of type text

> BEGIN

> INSERT INTO t VALUES (11, 12, 'f')

! INSERT INTO t SELECT * FROM (
    VALUES (13, 14, 'g')
  );
contains:cannot be run inside a transaction block

> COMMIT

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b
5 6 c
7 8 d
9 10 e

> BEGIN

! INSERT INTO t SELECT * FROM (
    VALUES (11, 12, 'f')
  );
contains:cannot be run inside a transaction block

> COMMIT

> CREATE MATERIALIZED VIEW v (a, b, c) AS SELECT 11, 12::real, 'f';

! INSERT INTO t SELECT * FROM v;
contains:invalid selection

# Table check descends into select targets
! INSERT INTO t (i, f, t) SELECT column1, column2, column3
    FROM ( VALUES (11, 12, 'f') )
    LEFT JOIN (
        SELECT a, b, c FROM v
    ) AS y
    ON y.a = column1
contains:invalid selection

# Multiple connections

> CREATE TABLE c (a int);
> INSERT INTO c VALUES (1);

> CREATE TABLE s (a int);

$ postgres-connect name=writer url=postgres://materialize:materialize@${testdrive.materialized-addr}

# In-flight txns don't affect updates/deletes, and vice versa

$ postgres-execute connection=writer
BEGIN;
INSERT INTO s VALUES (2);

> INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1

$ postgres-execute connection=writer

INSERT INTO s VALUES (3);
COMMIT;

> INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1
2
3

> BEGIN;
> SELECT a FROM c
1
2
3

$ postgres-execute connection=writer
INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1
2
3

> COMMIT;

# Every value from s should be duplicated in c
> SELECT a FROM s
2
3

> SELECT a FROM c;
1
2
2
3
3
