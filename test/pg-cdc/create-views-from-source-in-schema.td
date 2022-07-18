# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that CREATE VIEWS can be used to direct the tables to another schema
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE TABLE t2 (f1 INTEGER);
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t2 VALUES (2);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> DROP SCHEMA IF EXISTS schema1

> CREATE SCHEMA schema1

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

# Wait for snapshot to be complete
> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source (t1 AS schema1.t1, t2 AS schema1.t2);

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 SELECT * FROM t2;

> SELECT * FROM schema1.t1;
1
1

> SELECT * FROM schema1.t2;
2
2

> DROP SCHEMA schema1 CASCADE;
