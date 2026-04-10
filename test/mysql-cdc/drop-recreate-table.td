# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that dropping and recreating a MySQL table with the same schema in a
# single transaction produces a definite error in Materialize, even though
# the resulting schema is identical. The table gets a new table_id in the
# MySQL binlog, so Materialize should detect this as a table drop.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE drop_recreate (f1 INTEGER, f2 TEXT);
INSERT INTO drop_recreate VALUES (1, 'one');
INSERT INTO drop_recreate VALUES (2, 'two');

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CLUSTER drop_recreate_cluster SIZE '${arg.default-replica-size}';

> BEGIN
> CREATE SOURCE mz_source IN CLUSTER drop_recreate_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE drop_recreate FROM SOURCE mz_source (REFERENCE public.drop_recreate);
> COMMIT

# Verify the initial data is visible.
> SELECT * FROM drop_recreate;
1 one
2 two


# To make the test more deterministic, we pause MZ's ability to see replication events
# while the upstream table is dropped and recreated.
> ALTER CLUSTER drop_recreate_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'drop_recreate_cluster';
0

# Drop and recreate the table with the same schema in a single transaction.
$ mysql-execute name=mysql
BEGIN;
DROP TABLE drop_recreate;
CREATE TABLE drop_recreate (f1 INTEGER, f2 TEXT);
INSERT INTO drop_recreate VALUES (3, 'three');
COMMIT;

> ALTER CLUSTER drop_recreate_cluster SET (REPLICATION FACTOR 1)

# The table was dropped and recreated, so Materialize should emit a definite error.
#! SELECT * FROM drop_recreate;
#contains:table was dropped

# INCORRECT BEHAVIOR: this violates correctness
> SELECT * FROM drop_recreate;
1 one
2 two
3 three


> DROP CLUSTER drop_recreate_cluster CASCADE;
