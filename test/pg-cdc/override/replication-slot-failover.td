# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_postgres_source_failover = false

! CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER);
contains:the FAILOVER option for PostgreSQL sources is not available

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_postgres_source_failover = true

> CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER);
> CREATE TABLE pk_table FROM SOURCE pg_source (REFERENCE pk_table);

> SELECT * FROM pk_table
1 one

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pk_table VALUES (2, 'two');

> SELECT * FROM pk_table
1 one
2 two
