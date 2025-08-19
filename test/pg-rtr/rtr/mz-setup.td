# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE table_a (x int, y int);
ALTER TABLE table_a REPLICA IDENTITY FULL;

CREATE TABLE table_b (x int, y int);
ALTER TABLE table_b REPLICA IDENTITY FULL;

INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 100);

INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 100);

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg_conn_1 TO POSTGRES (
    HOST toxiproxy,
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CONNECTION pg_conn_2 TO POSTGRES (
    HOST toxiproxy,
    PORT 4432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> BEGIN
> CREATE SOURCE pg_source1
  FROM POSTGRES CONNECTION pg_conn_1 (PUBLICATION 'mz_source');
> CREATE TABLE table_a FROM SOURCE pg_source1 (REFERENCE table_a);
> COMMIT

> BEGIN
> CREATE SOURCE pg_source2
  FROM POSTGRES CONNECTION pg_conn_2 (PUBLICATION 'mz_source');
> CREATE TABLE table_b FROM SOURCE pg_source2 (REFERENCE table_b);
> COMMIT

> CREATE TABLE t (a int);
> INSERT INTO t VALUES (1);

> CREATE MATERIALIZED VIEW sum AS
  SELECT sum(count)
  FROM (
      SELECT count(*) FROM table_a
      UNION ALL SELECT count(*) FROM table_b
      UNION ALL SELECT count(*) FROM t
  ) AS x;
