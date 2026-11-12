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
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (1, 'one');

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_postgres_source_failover = false

! CREATE SOURCE implied
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER);
contains:the FAILOVER option for PostgreSQL sources is not available

! CREATE SOURCE explicit_false
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER = false);
contains:the FAILOVER option for PostgreSQL sources is not available

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_postgres_source_failover = true

> CREATE SOURCE implied
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER);
> CREATE SOURCE explicit_true
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER = true);
> CREATE SOURCE explicit_false
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', FAILOVER = false);

# A source only creates its slot once it ingests a table.
> CREATE TABLE implied_t FROM SOURCE implied (REFERENCE pk_table);
> CREATE TABLE explicit_true_t FROM SOURCE explicit_true (REFERENCE pk_table);
> CREATE TABLE explicit_false_t FROM SOURCE explicit_false (REFERENCE pk_table);

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pk_table VALUES (2, 'two');

> SELECT * FROM implied_t
1 one
2 two

> SELECT * FROM explicit_true_t
1 one
2 two

> SELECT * FROM explicit_false_t
1 one
2 two

> SELECT count(*) FROM mz_internal.mz_postgres_sources
3
