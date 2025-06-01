# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that tables with REPLICA INT IDENTITY DEFAULT or NOTHING will error
# out the source rather than cause wrong data or panics
#
# INT IDENTITY DEFAULT is the same as USING INDEX (t1_pkey)
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'test') BEGIN ALTER DATABASE test SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE test; END;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

# TODO: Reenable when database-issues#4231 is fixed
$ skip-if
SELECT true

$ sql-server-execute name=sql-server
CREATE TABLE identity_default_key_update (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_key_update REPLICA INT IDENTITY DEFAULT;

CREATE TABLE identity_default_nokey_update (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_nokey_update REPLICA INT IDENTITY DEFAULT;

CREATE TABLE identity_default_delete (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_default_delete REPLICA INT IDENTITY DEFAULT;

CREATE TABLE identity_nothing (key_col INTEGER PRIMARY KEY, nokey_col INTEGER);
ALTER TABLE identity_nothing REPLICA INT IDENTITY NOTHING;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

# Make sure that the above sources are fully instantiated so that the DML statements below
# are sent as actual replication events post-snapshot.

> SELECT COUNT(*) FROM identity_default_key_update;
0

$ sql-server-execute name=sql-server
INSERT INTO identity_default_key_update VALUES (1, 1);
UPDATE identity_default_key_update SET key_col = 2;

INSERT INTO identity_default_nokey_update VALUES (1, 1);
UPDATE identity_default_nokey_update SET nokey_col = 2;

INSERT INTO identity_default_delete VALUES (1, 1);
DELETE FROM identity_default_delete;

INSERT INTO identity_nothing VALUES (1, 1);

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
