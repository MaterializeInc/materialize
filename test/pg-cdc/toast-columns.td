# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that we can ingest unchanged toasted values
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

# Insert data pre-snapshot by generating 16kB of uncompressible data to force
# TOASTed storage by concatenating 1024 random MD5 hashes, each being 128bit
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (a int, b text);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 SELECT 1, string_agg(md5(random()::text), '') FROM generate_series(1, 1024);
INSERT INTO t1 SELECT 2, string_agg(md5(random()::text), '') FROM generate_series(1, 1024);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT a, length(b) FROM t1;
1 32768
2 32768

# Update the rows without touching the TOASTed column
$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE t1 SET a = 3;

> SELECT a, length(b) FROM t1;
3 32768
3 32768
