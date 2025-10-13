# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

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
CREATE SCHEMA public;
DROP SCHEMA IF EXISTS other CASCADE;
CREATE SCHEMA other;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t (f1 INT);
INSERT INTO t VALUES (1);
ALTER TABLE t REPLICA IDENTITY FULL;

CREATE TABLE other.t (f1 INT);
INSERT INTO other.t VALUES (1);
ALTER TABLE other.t REPLICA IDENTITY FULL;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

! CREATE TABLE t FROM SOURCE mz_source (REFERENCE t);
contains: reference t is ambiguous

> CREATE TABLE t FROM SOURCE mz_source (REFERENCE public.t);

! CREATE TABLE t FROM SOURCE mz_source (REFERENCE other.t);
contains:catalog item 't' already exists

> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE other.t);

> DROP TABLE t;

> CREATE TABLE x FROM SOURCE mz_source (REFERENCE public.t);

# multiple subsources may refer to the same table postgres.public.t
> CREATE TABLE Y FROM SOURCE mz_source (REFERENCE public.t);

> DROP TABLE x;

> CREATE TABLE t FROM SOURCE mz_source (REFERENCE other.t);

> SHOW sources
 mz_source          postgres  quickstart  ""

> SHOW TABLES
t                  ""
t2                 ""
y                  ""

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA other CASCADE;

# TODO: database-issues#8708
# > SET statement_timeout = '5s'

# ! SELECT * FROM t
# contains:statement timeout
