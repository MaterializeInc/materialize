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


> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER mz_user,
    PASSWORD SECRET pgpass
  )

> BEGIN
> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE foo FROM SOURCE mz_source (REFERENCE foo);
> COMMIT

# Wait for the initial snapshot to be ingested, note we cannot see the second row
> SELECT * FROM foo
1 a true

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO foo (val, visible) VALUES ('c', true);
DELETE FROM foo WHERE id = 2;

# Boom
> SELECT * FROM foo
1 a true
3 c true
