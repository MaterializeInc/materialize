# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for ss-159: an empty `EXCLUDE COLUMNS ()` on the legacy
# `CREATE SOURCE .. FOR TABLES` (subsource-creating) syntax is treated as
# though the option were omitted and must succeed.

# EXCLUDE COLUMNS on the legacy Postgres source syntax was introduced in
# v0.162, so skip on older binaries in the multi-version upgrade path that
# cannot parse the option.

$ skip-if
SELECT mz_version_num() < 16200;

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
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY, f2 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1, 'one'), (2, 'two');
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', EXCLUDE COLUMNS ())
  FOR TABLES (public.t1);

> SELECT * FROM t1;
1 one
2 two
