# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SECRET IF EXISTS mysqlpass CASCADE;
> DROP CONNECTION IF EXISTS mysql_conn CASCADE;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST "${arg.mysql-source-host}",
    PORT ${arg.mysql-source-port},
    USER root,
    PASSWORD SECRET mysqlpass
  )

> DROP SOURCE IF EXISTS mz_source CASCADE;

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t0 FROM SOURCE mz_source (REFERENCE public.t0);
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> CREATE TABLE alter_fail_drop_constraint FROM SOURCE mz_source (REFERENCE public.alter_fail_drop_constraint);
> CREATE TABLE alter_fail_drop_col FROM SOURCE mz_source (REFERENCE public.alter_fail_drop_col);
