# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test various variants of integer types

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

CREATE TABLE t1 (f1 SMALLINT, f2 INTEGER, f3 BIGINT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES (-32768, -2147483648, -9223372036854775808);
INSERT INTO t1 VALUES (32767, 2147483647, 9223372036854775807);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3) FROM t1 LIMIT 1;
smallint integer bigint

> SELECT * FROM t1;
-32768 -2147483648 -9223372036854775808
32767 2147483647 9223372036854775807
-32768 -2147483648 -9223372036854775808
32767 2147483647 9223372036854775807
