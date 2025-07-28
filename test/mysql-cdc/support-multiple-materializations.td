# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test that any replication slot can only be materialized once
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Insert data pre-snapshot
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE t1 (id SERIAL PRIMARY KEY, f1 BOOLEAN);

CREATE TABLE t2 (id SERIAL PRIMARY KEY, t1_id BIGINT UNSIGNED REFERENCES t1(id), name VARCHAR(32));

INSERT INTO t1(f1) VALUES (true),(false);

INSERT INTO t2(t1_id, name) VALUES (1, 'example');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);


> CREATE MATERIALIZED VIEW t1_mat AS
  SELECT * FROM t1

> SELECT id, f1 FROM t1_mat;
1 1
2 0

> CREATE MATERIALIZED VIEW t1_mat_dupe AS
  SELECT * FROM t1

> DROP MATERIALIZED VIEW t1_mat;

> DROP SOURCE mz_source CASCADE;

# verify that dropping things allows recreation

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);

> CREATE MATERIALIZED VIEW joiner AS
  SELECT t2.id, t1.f1, t2.name
  FROM t1
  JOIN t2
  ON   t1.id = t2.t1_id

> SELECT * FROM joiner;
1 1 example
