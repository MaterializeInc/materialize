# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# MONEY data type supported only as TEXT
#

> CREATE SECRET pgpass AS 'yugabyte'
> CREATE CONNECTION pg TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )

# Insert data pre-snapshot
$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
ALTER USER yugabyte WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 MONEY);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES (-92233720368547758.08), (+92233720368547758.07);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source', TEXT COLUMNS (t1.f1))
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
"text"

> SELECT * FROM t1;
"$92,233,720,368,547,758.07"
"$92,233,720,368,547,758.07"
"-$92,233,720,368,547,758.08"
"-$92,233,720,368,547,758.08"
