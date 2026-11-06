# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Set up a MySQL source with one hydrated table. The upstream tables that are
# added to the source later already exist and have data. Each worker reads its
# tables in order of their qualified names. Even when one worker reads both,
# `t_alongside` finishes its read before the `mysql_snapshot_pause` failpoint
# holds `t_paused`.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t_before (f1 INTEGER);
INSERT INTO t_before VALUES (1);
CREATE TABLE t_alongside (f1 INTEGER);
INSERT INTO t_alongside VALUES (10), (20);
CREATE TABLE t_paused (f1 INTEGER);
INSERT INTO t_paused VALUES (100), (200);

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS mysqlpass CASCADE;
> DROP CONNECTION IF EXISTS mysql_conn CASCADE;
> DROP CLUSTER IF EXISTS storage CASCADE;

> CREATE CLUSTER storage REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      STORAGE ADDRESSES ['clusterd1:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102'],
      WORKERS 4
    )
  )

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t_before FROM SOURCE mz_source (REFERENCE public.t_before);

> SELECT * FROM t_before
1
