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

> CREATE SECRET pgpass AS 'yugabyte'
> CREATE CONNECTION pg TO YUGABYTE (
    HOST yugabyte,
    PORT 5433,
    DATABASE yugabyte,
    USER yugabyte,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
ALTER USER yugabyte WITH replication;
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
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR ALL TABLES;
contains:PUBLICATION mz_source_empty is empty

! CREATE SOURCE "mz_source_empty"
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source_empty')
  FOR TABLES (t1);
contains:PUBLICATION mz_source_empty is empty

! CREATE SOURCE "mz_source_empty"
  FROM YUGABYTE CONNECTION pg (PUBLICATION mz_source);
contains:missing TABLES specification

! CREATE SOURCE mz_source
  FROM YUGABYTE CONNECTION pg (PUBLICATION mz_source)
  FOR SCHEMAS (dne);
contains:database postgres missing referenced schemas
detail:missing schemas: dne

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
DROP SCHEMA other CASCADE;
