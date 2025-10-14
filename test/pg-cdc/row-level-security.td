# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# MZ can successfully snapshot and replication tables with Row Level Security

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

DROP USER IF EXISTS mz_user;
CREATE USER mz_user WITH PASSWORD 'postgres' LOGIN REPLICATION;

CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO mz_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mz_user;

CREATE TABLE foo (id serial primary key, val text, visible bool);
ALTER TABLE foo REPLICA IDENTITY FULL;
ALTER TABLE foo ENABLE ROW LEVEL SECURITY;
INSERT INTO foo (val, visible) VALUES ('a', true), ('b', false);

CREATE POLICY restrict_mzuser ON foo FOR SELECT TO mz_user USING (visible = TRUE);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE CLUSTER rls SIZE = 'scale=1,workers=1', REPLICATION FACTOR = 1;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER mz_user,
    PASSWORD SECRET pgpass
  )


# Policy that affects the user is detected
> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER rls
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
! CREATE TABLE foo FROM SOURCE mz_source (REFERENCE foo);
contains:one or more tables requires BYPASSRLS
> ROLLBACK;

# Policy for public instead of a specific user is also detected
$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP POLICY restrict_mzuser ON foo;
CREATE POLICY restrict_mzuser ON foo FOR SELECT USING (visible = TRUE);

> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER rls
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
! CREATE TABLE foo FROM SOURCE mz_source (REFERENCE foo);
contains:one or more tables requires BYPASSRLS
> ROLLBACK;

# if bypassrls is set, source can be created
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER ROLE mz_user BYPASSRLS;

> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER rls
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE foo FROM SOURCE mz_source (REFERENCE foo);
> COMMIT;

# Initial snapshot shows all rows
> SELECT * FROM foo
1 a true
2 b false

# Updates are processed without error
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO foo (val, visible) VALUES ('c', true);
DELETE FROM foo WHERE id = 2;
UPDATE foo SET val = 'apple' WHERE id = 1;

> SELECT * FROM foo
1 apple true
3 c true

## Test that MZ also detects RLS restrictions during snapshot, causing the dataflow to emit
## a transient error.
> ALTER CLUSTER rls SET (REPLICATION FACTOR = 0);

> SELECT status
  FROM mz_internal.mz_source_statuses
  WHERE name = 'mz_source';
paused

# purification has happened, and the source was ok
> CREATE TABLE bar FROM SOURCE mz_source (REFERENCE foo);

# when the snapshot starts, RLS will again be in place.  The dataflow should return a
# transient error.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER ROLE mz_user NOBYPASSRLS;

> ALTER CLUSTER rls SET (REPLICATION FACTOR = 1);

> SELECT status, error LIKE '%one or more tables requires BYPASSRLS%'
  FROM mz_internal.mz_source_statuses
  WHERE name = 'bar';
stalled true

# Enabling BYPASSRLS allows the snapshot to continue without recreating the source.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER ROLE mz_user BYPASSRLS;

> SELECT * FROM bar;
1 apple true
3 c true

> SELECT * FROM foo;
1 apple true
3 c true
