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
contains:PUBLICATION mz_source_empty is empty

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR TABLES (t1);
contains:PUBLICATION mz_source_empty is empty

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = false

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);
contains:missing TABLES specification

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = true

! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source)
  FOR SCHEMAS (dne);
contains:no tables found in referenced schemas: "dne"

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA other CASCADE;
