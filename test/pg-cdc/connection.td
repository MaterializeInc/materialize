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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = true
ALTER SYSTEM SET enable_connection_validation_syntax = true

> CREATE SECRET pgpass AS 'postgres'

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    BROKER '${testdrive.kafka-addr}'
  )
contains:POSTGRES connections do not support BROKER values

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
  WITH (VALIDATE = true)
contains:role "no_such_user" does not exist

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
  WITH (VALIDATE = false)

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

# We know this connection is invalid
! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;
contains:role "no_such_user" does not exist

> ALTER CONNECTION pg SET USER postgres
    WITH (VALIDATE = true);

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM pk_table;
1 one
2 two

! ALTER CONNECTION pg SET PORT 1 WITH (VALIDATE = true)
contains:Connection refused

> ALTER CONNECTION pg SET PORT 1 WITH (VALIDATE = false)

