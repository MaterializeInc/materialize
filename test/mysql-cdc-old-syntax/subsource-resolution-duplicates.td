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


> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
DROP DATABASE IF EXISTS other;
CREATE DATABASE public;
CREATE DATABASE other;
USE public;

CREATE TABLE t (f1 INT);
INSERT INTO t VALUES (1);

CREATE TABLE other.t (f1 INT);
INSERT INTO other.t VALUES (1);

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;
contains:multiple subsources would be named t
detail:referenced tables with duplicate name: other.t, public.t

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.t AS x, public.t as Y);
contains:multiple subsources refer to table public.t
detail: subsources referencing table: x, y

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (other);

> SHOW sources
 mz_source          mysql     quickstart  ""
 mz_source_progress progress  <null>      ""
 t                  subsource quickstart  ""
