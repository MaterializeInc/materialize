# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for the parallel MySQL snapshot (SS-97): a table dropped
# upstream before its snapshot is read must fail only that table's outputs, not
# the whole source. During setup the leader counts and locks every pending table
# before any data is read, so a dropped table makes that step fail with a
# definite TableDropped error that must not be published to healthy siblings.
#
# The drop is made deterministic with the replication-factor-0 trick: while the
# replica is down we register two new tables (both pending a fresh snapshot) and
# drop one upstream, then resume with the other still present.

$ set-sql-timeout duration=120s

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
CREATE TABLE anchor (f1 INTEGER);
INSERT INTO anchor VALUES (1);

> CREATE CLUSTER c SIZE '${arg.default-replica-size}';

> BEGIN
> CREATE SOURCE mz_source IN CLUSTER c FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE anchor FROM SOURCE mz_source (REFERENCE public.anchor);
> COMMIT

# Confirm the source is up before pausing it.
> SELECT * FROM anchor;
1

> ALTER CLUSTER c SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'c';
0

# Two new tables with a single-column PK so the leader splits them into
# per-worker ranges. Both are pending a fresh snapshot when the replica resumes.
$ mysql-execute name=mysql
CREATE TABLE survivor (id INTEGER PRIMARY KEY);
INSERT INTO survivor VALUES (1), (2), (3), (4);
CREATE TABLE dropme (id INTEGER PRIMARY KEY);
INSERT INTO dropme VALUES (1), (2);

> CREATE TABLE survivor FROM SOURCE mz_source (REFERENCE public.survivor);
> CREATE TABLE dropme FROM SOURCE mz_source (REFERENCE public.dropme);

$ mysql-execute name=mysql
DROP TABLE dropme;

> ALTER CLUSTER c SET (REPLICATION FACTOR 1)

# The healthy sibling must finish its snapshot. Under the bug the dropped table
# aborts the whole source and this returns a definite error instead of rows.
> SELECT * FROM survivor;
1
2
3
4

# Only the dropped table is errored.
! SELECT * FROM dropme;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;
