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

# Insert data into MySQL that can't be decoded using native types and must be decoded
# as a TEXT COLUMN
# NOTE: We need to use `sql_mode = ''` to have MySQL allow the 0000-00-00 dates which it
# correctly disallows by default in newer versions, but used to allow in previous ones.

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 JSON, f2 ENUM('small', 'medium', 'large'), f3 YEAR, f4 DATE, f5 DATE, f6 DATE, f7 DATETIME, f8 DATETIME, f9 DATETIME(4));
SET SESSION sql_mode = '';
INSERT INTO t1 VALUES (CAST('{"bar": "baz", "balance": 7.77, "active": false, "nest": {"birds": ["seagull", "robin"]}}' AS JSON), 'large', 2024, '0000-00-00', '2024-00-01', '2024-01-00', '0000-00-00 00:00:00',  '0000-00-00 00:00:00.000000',  '0000-00-00 00:00:00.000000');

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE t1 FROM SOURCE da (REFERENCE public.t1)
  WITH (TEXT COLUMNS = (f1, f2, f3, f4, f5, f6, f7, f8, f9));
> COMMIT

# Insert the same data post-snapshot
$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
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

> SELECT f4 FROM t1;
0000-00-00
0000-00-00

> SELECT f5 FROM t1;
2024-00-01
2024-00-01

> SELECT f6 FROM t1;
2024-01-00
2024-01-00

> SELECT f7 FROM t1;
"0000-00-00 00:00:00"
"0000-00-00 00:00:00"

> SELECT f8 FROM t1;
"0000-00-00 00:00:00"
"0000-00-00 00:00:00"

> SELECT f9 FROM t1;
"0000-00-00 00:00:00.0000"
"0000-00-00 00:00:00.0000"

$ set-regex match="DETAILS = '[a-f0-9]+'" replacement=<DETAILS>

> SHOW CREATE TABLE t1;
materialize.public.t1 "CREATE TABLE materialize.public.t1 (f1 pg_catalog.text, f2 pg_catalog.text, f3 pg_catalog.text, f4 pg_catalog.text, f5 pg_catalog.text, f6 pg_catalog.text, f7 pg_catalog.text, f8 pg_catalog.text, f9 pg_catalog.text) FROM SOURCE materialize.public.da (REFERENCE = public.t1) WITH (TEXT COLUMNS = (f1, f2, f3, f4, f5, f6, f7, f8, f9), <DETAILS>);"

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

> CREATE SOURCE da_other
  FROM MYSQL CONNECTION mysqc;
! CREATE TABLE t1 FROM SOURCE da_other (REFERENCE public.t1) WITH (TEXT COLUMNS (f1));
contains: unsupported type
