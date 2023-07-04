# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that subsources are created in the source schema if provided
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

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

> CREATE SCHEMA a;
> CREATE SOURCE a.mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (t1);

> SELECT * FROM a.t1;
1

> CREATE SCHEMA another;
> CREATE SOURCE another.mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM another.t1;
1

> SELECT * FROM another.t2;
5

> CREATE SCHEMA foo;
> CREATE SCHEMA bar;
> CREATE SOURCE foo.mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (t1 as table1, t2 as bar.table2);

! SELECT * FROM table1;
contains: unknown

# table1 gets created in source schema foo because it doesn't have any prefix
> SELECT * FROM foo.table1;
1

! SELECT * FROM foo.table2;
contains: unknown

# table2 gets created in mentioned bar because it does have a prefix
> SELECT * FROM bar.table2;
5
