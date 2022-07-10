# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test various variants of DECIMAL
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

CREATE TABLE t1 (f1 DECIMAL(20, 0), f2 DECIMAL(20, 20), f3 DECIMAL(20,10), f4 NUMERIC);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('99999999999999999999', '0.99999999999999999999', '9999999999.9999999999', '9999999999.9999999999');
INSERT INTO t1 VALUES ('-99999999999999999999', '-0.99999999999999999999', '-9999999999.9999999999', '-9999999999.9999999999');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
numeric numeric numeric numeric

> SELECT * FROM t1;
99999999999999999999 0.99999999999999999999 9999999999.9999999999 9999999999.9999999999
-99999999999999999999 -0.99999999999999999999 -9999999999.9999999999 -9999999999.9999999999
99999999999999999999 0.99999999999999999999 9999999999.9999999999 9999999999.9999999999
-99999999999999999999 -0.99999999999999999999 -9999999999.9999999999 -9999999999.9999999999
