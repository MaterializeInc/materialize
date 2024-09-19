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

$ skip-if
SELECT mz_version_num() >= 6000;

## Create a MySQL source to ensure that its subsources can be upgraded through
## migrations in v0.110.0

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public_1;
CREATE DATABASE public_1;
USE public_1;
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 TEXT, f3 ENUM ('var0', 'var1'));
INSERT INTO table_a VALUES (1, 'one', 'var0');
INSERT INTO table_a VALUES (2, 'two', 'var1');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn (
   TEXT COLUMNS (public_1.table_a.f3)
  )
  FOR TABLES (public_1.table_a);


## Create a Postgres source to ensure that its subsources can be upgraded through
## migrations in v0.110.0

> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
> CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA public_1 CASCADE;
CREATE SCHEMA public_1;
ALTER USER postgres WITH replication;
DROP PUBLICATION IF EXISTS mz_source_1;
CREATE PUBLICATION mz_source_1 FOR ALL TABLES;
CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE public_1.table_a (pk INTEGER PRIMARY KEY, f2 TEXT, f3 an_enum, f4 NUMERIC(50));
INSERT INTO public_1.table_a VALUES (1, 'one', 'var0', 500.0);
ALTER TABLE public_1.table_a REPLICA IDENTITY FULL;
INSERT INTO public_1.table_a VALUES (2, 'two', 'var1', 1000.0);


> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (
    PUBLICATION 'mz_source_1',
    TEXT COLUMNS [public_1.table_f.f2, public_1.table_f.f4]
  )
  FOR TABLES (public_1.table_a);


## Create a Loadgen source to ensure that its subsources can be upgraded through
## migrations in v0.110.0

> CREATE SOURCE demo_1
  FROM LOAD GENERATOR AUCTION (TICK INTERVAL '500ms')

> CREATE TABLE accounts FROM SOURCE demo_1 (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE demo_1 (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE demo_1 (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE demo_1 (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE demo_1 (REFERENCE users);
