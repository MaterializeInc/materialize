# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test is expected to run with fewer than 4 replication slots.

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

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

> BEGIN
> CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE "pk_table" FROM SOURCE pg_source (REFERENCE "pk_table");
> COMMIT

! CREATE SOURCE pg_source_2
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
contains:POSTGRES source validation: PostgreSQL server has insufficient number of replication slots available
