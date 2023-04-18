# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests that empty publication and missing for tables clauses errors correctly
#

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
DROP SCHEMA IF EXISTS other CASCADE;
CREATE SCHEMA other;

DROP PUBLICATION IF EXISTS mz_source_empty;
CREATE PUBLICATION mz_source_empty;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t (f1 int);
ALTER TABLE t REPLICA IDENTITY FULL;

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR ALL TABLES;
exact: PostgreSQL PUBLICATION mz_source_empty is empty

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR TABLES (t1);
exact: PostgreSQL PUBLICATION mz_source_empty is empty

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);
exact: multi-output sources require a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause

! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source)
  FOR SCHEMAS (dne);
exact: FOR SCHEMAS (..) included dne, but PostgreSQL database has no schema with that name
