# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-532: dropping and re-adding a UNIQUE constraint with
# the same name and columns is invisible to the MySQL source when the source
# only catches up after the constraint is back.
#
# The source verifies a table's schema against the *current* upstream schema
# when it processes the binlog event for a DDL statement, and MySQL keys have
# no identity beyond their name and columns. So when the drop and the re-add
# have both happened by the time the drop's binlog event is processed, the
# schema check passes, and rows written in the window that violated the key
# are ingested as if the key held at those timestamps.
#
# Dropping and recreating the table itself is covered by
# drop-recreate-table.td: the MySQL source parses the DROP TABLE statement
# from the binlog and errors the table permanently.
#
# This file asserts the current behavior. The outcome marked SS-532 is the
# behavior the issue wants changed.

$ set-sql-timeout duration=60s

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

CREATE TABLE recreate_uq (id INT NOT NULL PRIMARY KEY, wallet VARCHAR(64) NOT NULL, CONSTRAINT recreate_uq_wallet UNIQUE (wallet));
INSERT INTO recreate_uq VALUES (1, 'a');

> CREATE CLUSTER recreate_cluster SIZE '${arg.default-replica-size}';

> CREATE SOURCE mz_source IN CLUSTER recreate_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE recreate_uq FROM SOURCE mz_source (REFERENCE public.recreate_uq);

> SELECT * FROM recreate_uq;
1 a

# Materialize planned with the key, so a DISTINCT over it is free.
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT wallet FROM recreate_uq;
Explained Query:
  Project (#1)
    ReadStorage materialize.public.recreate_uq

Source materialize.public.recreate_uq

Target cluster: quickstart

# Stop the source so it only catches up once the constraint is back. MySQL
# DDL is not transactional, so this stands in for a source that is lagging
# behind the migration.
> ALTER CLUSTER recreate_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'recreate_cluster';
0

# Drop the constraint, write and remove a row that violates it, then re-add
# it under the same name over the same columns.
$ mysql-execute name=mysql
ALTER TABLE recreate_uq DROP INDEX recreate_uq_wallet;
INSERT INTO recreate_uq VALUES (2, 'a');
DELETE FROM recreate_uq WHERE id = 2;
ALTER TABLE recreate_uq ADD CONSTRAINT recreate_uq_wallet UNIQUE (wallet);
INSERT INTO recreate_uq VALUES (3, 'b');

> ALTER CLUSTER recreate_cluster SET (REPLICATION FACTOR 1)

# SS-532: the drop and the re-add are invisible. The table keeps ingesting,
# and at the timestamps between the insert and the delete of id 2 it held
# two rows with wallet 'a' under a schema that promised wallet is unique.
> SELECT * FROM recreate_uq;
1 a
3 b

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'recreate_uq';
running

> DROP CLUSTER recreate_cluster CASCADE;
