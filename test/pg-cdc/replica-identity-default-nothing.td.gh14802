# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that tables with REPLICA IDENTITY DEFAULT or NOTHING will error
# out the source rather than cause wrong data or panics
#
# IDENTITY DEFAULT is the same as USING INDEX (t1_pkey)
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE identity_default_key_update (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_key_update REPLICA IDENTITY DEFAULT;

CREATE TABLE identity_default_nokey_update (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_nokey_update REPLICA IDENTITY DEFAULT;

CREATE TABLE identity_default_delete (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_delete REPLICA IDENTITY DEFAULT;

CREATE TABLE identity_nothing (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_nothing REPLICA IDENTITY NOTHING;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

# Make sure that the above sources are fully instantiated so that the DML statements below
# are sent as actual replication events post-snapshot.

> SELECT COUNT(*) FROM mz_source;
0

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO identity_default_key_update VALUES (1, 1);
UPDATE identity_default_key_update SET key_col = 2;

INSERT INTO identity_default_nokey_update VALUES (1, 1);
UPDATE identity_default_nokey_update SET nokey_col = 2;

INSERT INTO identity_default_delete VALUES (1, 1);
DELETE FROM identity_default_delete;

INSERT INTO identity_nothing VALUES (1, 1);

> CREATE VIEWS FROM SOURCE mz_source;
> CREATE DEFAULT INDEX ON identity_default_key_update;
> CREATE DEFAULT INDEX ON identity_default_nokey_update;
> CREATE DEFAULT INDEX ON identity_default_delete;
> CREATE DEFAULT INDEX ON identity_nothing;

! SELECT * FROM identity_default_key_update;
contains:Old row missing from replication stream for table with OID

! SELECT * FROM identity_default_nokey_update;
contains:Old row missing from replication stream for table with OID

! SELECT * FROM identity_default_delete;
contains:Old row missing from replication stream for table with OID

! SELECT * FROM identity_nothing;
contains:Old row missing from replication stream for table with OID
