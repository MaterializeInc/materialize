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
