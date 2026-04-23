# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that CREATE TABLE FROM SOURCE fails when binlog_row_metadata is not FULL.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = MINIMAL;
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES (1, 'hello');

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

! CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
contains: binlog_row_metadata

# Restore to a clean state so other tests are unaffected.
$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = FULL;
