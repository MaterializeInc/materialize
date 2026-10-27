# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: a primary-key column renamed upstream between source planning and
# the initial snapshot must surface as a per-output definite error, not wedge the whole source.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE rename_pk (id INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO rename_pk VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (6, 'six'), (7, 'seven'), (8, 'eight');

CREATE TABLE healthy (id INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO healthy VALUES (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CLUSTER rename_pk_cluster SIZE '${arg.default-replica-size}';

> CREATE SOURCE mz_source IN CLUSTER rename_pk_cluster FROM MYSQL CONNECTION mysql_conn;

> ALTER CLUSTER rename_pk_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'rename_pk_cluster';
0

> CREATE TABLE rename_pk FROM SOURCE mz_source (REFERENCE public.rename_pk);
> CREATE TABLE healthy FROM SOURCE mz_source (REFERENCE public.healthy);

$ mysql-execute name=mysql
USE public;
ALTER TABLE rename_pk CHANGE id id2 INT NOT NULL;

> ALTER CLUSTER rename_pk_cluster SET (REPLICATION FACTOR 1)

> SELECT * FROM healthy
1 one
2 two
3 three

! SELECT * FROM rename_pk
contains:incompatible schema change

> DROP CLUSTER rename_pk_cluster CASCADE;
