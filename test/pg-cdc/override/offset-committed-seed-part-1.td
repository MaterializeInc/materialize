# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A hydrated table whose committed offset a later seed could undercut.

$ set-sql-timeout duration=60s

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
CREATE TABLE t1 (id SERIAL PRIMARY KEY, v TEXT);
CREATE TABLE t2 (id SERIAL PRIMARY KEY, v TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t1 (v) SELECT 'x' FROM generate_series(1, 100);
INSERT INTO t2 (v) SELECT 'y' FROM generate_series(1, 100);
CREATE PUBLICATION mz_source FOR TABLE t1, t2;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  WITH (TIMESTAMP INTERVAL '50ms');
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

> SELECT count(*) FROM t1
100

> SELECT bool_and(u.offset_committed > 0)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name = 't1'
true
