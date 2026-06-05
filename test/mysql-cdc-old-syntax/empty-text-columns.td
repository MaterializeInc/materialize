# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for ss-159: an empty `TEXT COLUMNS ()` on the legacy
# `CREATE SOURCE .. FOR TABLES` (subsource-creating) syntax is treated as
# though the option were omitted and must succeed. This differs from the
# `CREATE TABLE .. FROM SOURCE` syntax, where an empty set is rejected with
# `invalid TEXT COLUMNS option value: cannot be empty`.
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
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO t1 VALUES (1, 'one'), (2, 'two');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysqc (
    TEXT COLUMNS ()
  )
  FOR TABLES (public.t1);

> SELECT * FROM t1;
1 one
2 two
