# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that dropping an upstream table before its snapshot runs produces a
# definite error, not an infinite restart loop.
#
# The snapshot leader touches the tables BEFORE acquiring LOCK TABLES: it
# verifies output schemas against information_schema and then runs the PK-split
# probes (SELECT COUNT(*) and the OFFSET boundary walk). A table dropped
# upstream must surface as a definite `table was dropped` error from whichever
# of these steps sees it first, either the schema verification or, if the drop
# races past it, ER_NO_SUCH_TABLE from the probes or LOCK TABLES. Classifying
# it as transient would restart the dataflow into the same error forever.
#
# The drop is made deterministic with the REPLICATION FACTOR 0 -> add table ->
# drop upstream -> REPLICATION FACTOR 1 trick: with no replica the drop is
# guaranteed to land before the snapshot can run.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CLUSTER drop_snapshot_cluster SIZE '${arg.default-replica-size}';

# 1. Integer primary key (CAST boundary-literal path).

$ mysql-execute name=mysql
CREATE TABLE keep_int (f1 INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO keep_int VALUES (1, 'one');
CREATE TABLE drop_int (f1 INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO drop_int VALUES (1, 'one'), (2, 'two'), (3, 'three');

> BEGIN
> CREATE SOURCE src_int IN CLUSTER drop_snapshot_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE keep_int FROM SOURCE src_int (REFERENCE public.keep_int);
> COMMIT

> SELECT * FROM keep_int;
1 one

# Remove the replica so the newly added table cannot snapshot until we drop it.
> ALTER CLUSTER drop_snapshot_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'drop_snapshot_cluster';
0

> CREATE TABLE drop_int FROM SOURCE src_int (REFERENCE public.drop_int);

$ mysql-execute name=mysql
DROP TABLE drop_int;

> ALTER CLUSTER drop_snapshot_cluster SET (REPLICATION FACTOR 1)

! SELECT * FROM drop_int;
contains:table was dropped

# The already-snapshotted sibling is unaffected.
> SELECT * FROM keep_int;
1 one

> DROP SOURCE src_int CASCADE;

# 2. Text primary key (QUOTE boundary-literal path).

$ mysql-execute name=mysql
CREATE TABLE keep_text (f1 VARCHAR(32) PRIMARY KEY, f2 TEXT);
INSERT INTO keep_text VALUES ('a', 'one');
CREATE TABLE drop_text (f1 VARCHAR(32) PRIMARY KEY, f2 TEXT);
INSERT INTO drop_text VALUES ('a', 'one'), ('b', 'two'), ('c', 'three');

> BEGIN
> CREATE SOURCE src_text IN CLUSTER drop_snapshot_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE keep_text FROM SOURCE src_text (REFERENCE public.keep_text);
> COMMIT

> SELECT * FROM keep_text;
a one

> ALTER CLUSTER drop_snapshot_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'drop_snapshot_cluster';
0

> CREATE TABLE drop_text FROM SOURCE src_text (REFERENCE public.drop_text);

$ mysql-execute name=mysql
DROP TABLE drop_text;

> ALTER CLUSTER drop_snapshot_cluster SET (REPLICATION FACTOR 1)

! SELECT * FROM drop_text;
contains:table was dropped

> SELECT * FROM keep_text;
a one

> DROP SOURCE src_text CASCADE;

> DROP CLUSTER drop_snapshot_cluster CASCADE;
