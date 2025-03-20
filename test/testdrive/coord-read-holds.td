# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Make sure the Coordinator maintains read holds and that up-to-date objects are
# "immediately" queryable.

# Insert Postgres data
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE pg_t1 (a INT);
ALTER TABLE pg_t1 REPLICA IDENTITY FULL;

INSERT INTO pg_t1 VALUES (100), (101), (102);

CREATE PUBLICATION mz_source FOR ALL TABLES;

# Create a Postgres Source

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;


# Create a user table.

> CREATE TABLE t1 (a int);
> INSERT INTO t1 VALUES (201), (202), (203);

# TODO: Reenable when database-issues#8619 is fixed
$ skip-if
SELECT true

# Note: the important bit is making sure this EXPLAIN returns "can respond immediately: true"
$ set-regex match=(\d{13}|([\d\-]{10}\s[\d:.]{12})|u\d+) replacement=<NON_CONSTANT>
> EXPLAIN TIMESTAMP FOR SELECT COUNT(t1.a) FROM t1, pg_t1;
"                query timestamp: <NON_CONSTANT> (<NON_CONSTANT>)\n          oracle read timestamp: <NON_CONSTANT> (<NON_CONSTANT>)\nlargest not in advance of upper: <NON_CONSTANT> (<NON_CONSTANT>)\n                          upper:[<NON_CONSTANT> (<NON_CONSTANT>)]\n                          since:[<NON_CONSTANT> (<NON_CONSTANT>)]\n        can respond immediately: false\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <NON_CONSTANT> (<NON_CONSTANT>)\n\nsource materialize.public.pg_t1 (<NON_CONSTANT>, storage):\n                  read frontier:[<NON_CONSTANT> (<NON_CONSTANT>)]\n                 write frontier:[<NON_CONSTANT> (<NON_CONSTANT>)]\n\nsource materialize.public.t1 (<NON_CONSTANT>, storage):\n                  read frontier:[<NON_CONSTANT> (<NON_CONSTANT>)]\n                 write frontier:[<NON_CONSTANT> (<NON_CONSTANT>)]\n"
