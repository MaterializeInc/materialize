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

CREATE TABLE table_a (a INTEGER);
INSERT INTO table_a VALUES (1);
ALTER TABLE table_a REPLICA IDENTITY FULL;

CREATE TABLE table_b (a INTEGER);
INSERT INTO table_b VALUES (1);
ALTER TABLE table_b REPLICA IDENTITY FULL;

> CREATE SOURCE "mz_source"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (
    "table_a",
    "table_b"
  );

> SHOW SOURCES
mz_source             postgres  ${arg.default-storage-size}
mz_source_progress    subsource <null>
table_a               subsource <null>
table_b               subsource <null>

> SELECT envelope_type FROM mz_sources WHERE name = 'mz_source'
none

> SELECT subsources FROM mz_sources WHERE name = 'mz_source'
{u3,u4}
