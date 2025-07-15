# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test that views are not replicated
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# Insert data pre-snapshot
CREATE TABLE t1 (f1 BOOLEAN);
INSERT INTO t1 VALUES (true), (false);

CREATE VIEW v1 (f1) AS SELECT f1 FROM t1;

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

! CREATE TABLE v1 FROM SOURCE mz_source (REFERENCE public.v1);
contains:reference to public.v1 not found in source

! CREATE VIEW v1 FROM SOURCE mz_source (REFERENCE public.v1);
contains:Expected AS, found FROM
