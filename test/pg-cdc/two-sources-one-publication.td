# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that two sources reading from one publication is OK
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t2 VALUES (5);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> DROP SCHEMA IF EXISTS schema1

> CREATE SCHEMA schema1

> CREATE SOURCE mz_source1
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source1 (t1 AS t1_1, t2 AS t2_1);

> CREATE SOURCE mz_source2
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source2 (t1 AS t1_2, t2 AS t2_2);

> SELECT * FROM t1_1;
1

> SELECT * FROM t2_1;
5

> SELECT * FROM t1_2;
1

> SELECT * FROM t2_2;
5

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (3);
INSERT INTO t2 VALUES (6);
INSERT INTO t2 VALUES (7);

> SELECT * FROM t1_1;
1
2
3

> SELECT * FROM t2_1;
5
6
7

> SELECT * FROM t1_2;
1
2
3

> SELECT * FROM t2_2;
5
6
7

> DROP SCHEMA schema1 CASCADE;
