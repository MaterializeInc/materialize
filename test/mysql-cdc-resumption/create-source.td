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
    USER root,
    PASSWORD SECRET mysqlpass
  )

> DROP SOURCE IF EXISTS mz_source CASCADE;

# A multi-worker cluster so the snapshot of the t1pk/t2pk tables is split into
# per-worker PK ranges. On the default quickstart cluster (one worker) every
# table takes the single-worker whole-table path and the parallel snapshot
# logic is never exercised by the disruption scenarios.
> DROP CLUSTER IF EXISTS mysql_source_cluster CASCADE;
> CREATE CLUSTER mysql_source_cluster SIZE 'scale=1,workers=4', REPLICATION FACTOR 1;

> BEGIN
> CREATE SOURCE mz_source IN CLUSTER mysql_source_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t0 FROM SOURCE mz_source (REFERENCE public.t0);
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> CREATE TABLE t1pk FROM SOURCE mz_source (REFERENCE public.t1pk);
> CREATE TABLE t2pk FROM SOURCE mz_source (REFERENCE public.t2pk);
> CREATE TABLE alter_fail_drop_constraint FROM SOURCE mz_source (REFERENCE public.alter_fail_drop_constraint);
> CREATE TABLE alter_fail_drop_col FROM SOURCE mz_source (REFERENCE public.alter_fail_drop_col);
> COMMIT
