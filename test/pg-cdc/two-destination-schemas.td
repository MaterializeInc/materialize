# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that identically-named tables in two destination schemas can be
# successfully disambiguated and replicated
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP PUBLICATION IF EXISTS mz_source;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t2 VALUES (2);

CREATE PUBLICATION mz_source FOR ALL TABLES;


> DROP SCHEMA IF EXISTS schema1 CASCADE;
> DROP SCHEMA IF EXISTS schema2 CASCADE;

> CREATE SCHEMA schema1;
> CREATE SCHEMA schema2;

> DROP SOURCE IF EXISTS mz_source CASCADE;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (t1 AS schema1.t1, t2 AS schema2.t1);

> SELECT * FROM schema1.t1;
1

> SELECT * FROM schema2.t1;
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 SELECT * FROM t2;

> SELECT * FROM schema1.t1;
1
1

> SELECT * FROM schema2.t1;
2
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE t1;
DROP TABLE t2;

> DROP SOURCE mz_source CASCADE;
> DROP SCHEMA schema1 CASCADE;
> DROP SCHEMA schema2 CASCADE;
