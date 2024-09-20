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

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE tbl_a (pk INTEGER PRIMARY KEY);
INSERT INTO tbl_a VALUES (1);

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

! CREATE TABLE tbl_a FROM SOURCE mz_source (REFERENCE tbl_a);
contains:referenced items not tables with REPLICA IDENTITY FULL
detail:referenced items: public.tbl_a

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE tbl_a REPLICA IDENTITY FULL

> CREATE TABLE tbl_a FROM SOURCE mz_source (REFERENCE tbl_a);

> SELECT * FROM tbl_a;
1
