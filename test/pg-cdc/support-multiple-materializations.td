# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that any replication slot can only be materialized once
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass


# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (id SERIAL PRIMARY KEY, f1 BOOLEAN);
ALTER TABLE t1 REPLICA IDENTITY FULL;

CREATE TABLE t2 (id SERIAL PRIMARY KEY, t1_id INT REFERENCES t1(id), name VARCHAR);

INSERT INTO t1(f1) VALUES ('true'),('false');

INSERT INTO t2(t1_id, name) VALUES (1, 'example');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> CREATE MATERIALIZED VIEW t1_mat AS
  SELECT * FROM t1

> SELECT id, f1 FROM t1_mat;
1 true
2 false

> CREATE MATERIALIZED VIEW t1_mat_dupe AS
  SELECT * FROM t1

> CREATE MATERIALIZED VIEWS FROM SOURCE mz_source (t1 as t1_not_a_dupe, t2 as t2_not_a_dupe)

> DROP VIEW t1_mat;

> DROP SOURCE mz_source CASCADE;

# verify that dropping things allows recreation

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> CREATE MATERIALIZED VIEW joiner AS
  SELECT t2.id, t1.f1, t2.name
  FROM t1
  JOIN t2
  ON   t1.id = t2.t1_id

> SELECT * FROM joiner;
1 true example
