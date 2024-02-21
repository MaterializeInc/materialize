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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

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
CREATE TABLE t1 (f1 JSON);

INSERT INTO t1 VALUES (CAST('{"bar": "baz", "balance": 7.77, "active": false}' AS JSON));

> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc (
    TEXT COLUMNS (public.t1.f1)
  )
  FOR TABLES (public.t1);

# Insert the same data post-snapshot
$ mysql-execute name=mysql
USE public;
INSERT INTO t1 SELECT * FROM t1;

> SELECT f1::jsonb->>'balance' FROM t1;
7.77
7.77

#
# Validate that unsupported types error even as TEXT COLUMNS
#

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 BIT(5));

INSERT INTO t1 VALUES (b'11111');

! CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc (
    TEXT COLUMNS (public.t1.f1)
  )
  FOR TABLES (public.t1);
contains: unsupported type
