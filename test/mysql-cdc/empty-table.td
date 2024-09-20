# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20


#
# Test an empty table
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
CREATE TABLE empty_table (f1 BOOLEAN);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE empty_table FROM SOURCE mz_source (REFERENCE public.empty_table);

> SELECT COUNT(*) FROM empty_table;
0

> SELECT name, type, status FROM mz_internal.mz_source_statuses WHERE name in ('mz_source');
mz_source mysql running
