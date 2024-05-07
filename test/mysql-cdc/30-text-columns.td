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
CREATE TABLE t1 (f1 JSON, f2 ENUM('small', 'medium', 'large'), f3 YEAR);

INSERT INTO t1 VALUES (CAST('{"bar": "baz", "balance": 7.77, "active": false, "nest": {"birds": ["seagull", "robin"]}}' AS JSON), 'large', 2024);

> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc (
    TEXT COLUMNS (public.t1.f1, public.t1.f2, public.t1.f3)
  )
  FOR TABLES (public.t1);

# Insert the same data post-snapshot
$ mysql-execute name=mysql
USE public;
INSERT INTO t1 SELECT * FROM t1;

> SELECT f1::jsonb->>'balance' FROM t1;
7.77
7.77

> SELECT f2 FROM t1;
"large"
"large"

> SELECT f3 FROM t1;
"2024"
"2024"

# verify JSON representation is consistent between snapshot and replication
> SELECT f1 FROM t1;
"{\"bar\":\"baz\",\"nest\":{\"birds\":[\"seagull\",\"robin\"]},\"active\":false,\"balance\":7.77}"
"{\"bar\":\"baz\",\"nest\":{\"birds\":[\"seagull\",\"robin\"]},\"active\":false,\"balance\":7.77}"

> DROP SOURCE da CASCADE;

#
# Validate that unsupported types error even as TEXT COLUMNS
#

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 BIT(5));

INSERT INTO t1 VALUES (b'11111');

! CREATE SOURCE da_other
  FROM MYSQL CONNECTION mysqc (
    TEXT COLUMNS (public.t1.f1)
  )
  FOR TABLES (public.t1);
contains: unsupported type
