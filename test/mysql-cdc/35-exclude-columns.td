# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test mysql TEXT COLUMNS support
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 INTEGER, f2 GEOMETRY, f3 POINT, f4 VARCHAR(64));

INSERT INTO t1 VALUES (1, ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), ST_GeomFromText('POINT(1 1)'), 'test');

! CREATE SOURCE da_other
  FROM MYSQL CONNECTION mysqc
  FOR TABLES (public.t1);
contains: unsupported type

! CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc (
    EXCLUDE COLUMNS (t1.f2, t1.f3)
  )
  FOR TABLES (public.t1);
contains:invalid EXCLUDE COLUMNS option value: column name 't1.f2' must have at least a table qualification

> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc (
    EXCLUDE COLUMNS (public.t1.f2, public.t1.f3)
  )
  FOR TABLES (public.t1);

# Insert the same data post-snapshot
$ mysql-execute name=mysql
USE public;
INSERT INTO t1 SELECT * FROM t1;

> SELECT * FROM t1;
1 "test"
1 "test"

> SELECT f4 FROM t1;
"test"
"test"

> SHOW CREATE SOURCE t1;
materialize.public.t1 "CREATE SUBSOURCE \"materialize\".\"public\".\"t1\" (\"f1\" \"pg_catalog\".\"int4\", \"f4\" \"pg_catalog\".\"varchar\"(64)) OF SOURCE \"materialize\".\"public\".\"da\" WITH (EXTERNAL REFERENCE = \"public\".\"t1\", EXCLUDE COLUMNS = (\"f2\", \"f3\"))"

! SELECT f2 FROM t1;
contains:column "f2" does not exist

# Remove one of the ignored columns, and we should still error
$ mysql-execute name=mysql
ALTER TABLE t1 DROP COLUMN f2;

! select * from t1;
contains:incompatible schema change
