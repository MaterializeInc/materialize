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
CREATE SCHEMA public;
DROP SCHEMA IF EXISTS other CASCADE;
CREATE SCHEMA other;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t (f1 INT);
INSERT INTO t VALUES (1);
ALTER TABLE t REPLICA IDENTITY FULL;

CREATE TABLE other.t (f1 INT);
INSERT INTO other.t VALUES (1);
ALTER TABLE other.t REPLICA IDENTITY FULL;

! CREATE SOURCE mz_source
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;
contains:multiple subsources would be named t
detail:referenced tables with duplicate name: postgres.other.t, postgres.public.t

! CREATE SOURCE mz_source
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (public.t AS x, public.t as Y);
contains:multiple subsources refer to table postgres.public.t
detail: subsources referencing table: x, y

> CREATE SOURCE mz_source
  FROM YUGABYTE CONNECTION pg (PUBLICATION 'mz_source')
  FOR SCHEMAS (other);

> SHOW sources
 mz_source          postgres  quickstart  ""
 mz_source_progress progress  <null>      ""
 t                  subsource quickstart  ""

$ postgres-execute connection=postgres://yugabyte:yugabyte@yugabyte:5433/yugabyte
DROP SCHEMA other CASCADE;
