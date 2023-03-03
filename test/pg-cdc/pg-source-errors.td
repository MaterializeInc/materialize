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

DROP PUBLICATION IF EXISTS mz_source_empty;
CREATE PUBLICATION mz_source_empty;

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR ALL TABLES;
exact: FOR ALL TABLES is only valid for non-empty publication sources

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR TABLES (t1);
exact: FOR TABLES (..) is only valid for non-empty publication sources

! CREATE SOURCE "mz_source_empty"
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_empty');
exact: FOR ALL TABLES or FOR TABLES (..) must be specified
