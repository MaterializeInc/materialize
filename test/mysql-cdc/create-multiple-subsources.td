# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Validate that multiple subsources with different output types can be created concurrently.
# Previously, this would have resulted in a panic during snapshot as the column types were not
# equal.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE foo (id INTEGER, val YEAR);
INSERT INTO foo VALUES (1, 2011), (2,2022), (3, 2033);
COMMIT;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION myconn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> BEGIN;
> CREATE SOURCE mysrc FROM MYSQL CONNECTION myconn;
> CREATE TABLE foo1 FROM SOURCE mysrc (REFERENCE public.foo) WITH (EXCLUDE COLUMNS (val));
> CREATE TABLE foo2 FROM SOURCE mysrc (REFERENCE public.foo) WITH (TEXT COLUMNS (val));
> COMMIT;

> SELECT * FROM foo1;
1
2
3

> SELECT * FROM foo2;
1 "2011"
2 "2022"
3 "2033"
