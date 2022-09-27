# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Create a large snapshot and follow-up transactions
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

CREATE TABLE t1 (f1 SERIAL PRIMARY KEY, f2 INTEGER DEFAULT 0, f3 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 (f3) SELECT REPEAT('a', 1024 * 1024) FROM generate_series(1, 16);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

# > CREATE MATERIALIZED VIEW v1 AS SELECT f1 + 1, f2 FROM t1;

> CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM t1;

# Update data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
UPDATE t1 SET f2 = f2 + 1;
INSERT INTO t1 (f3) VALUES ('eof');

> SELECT * FROM v2;
17
