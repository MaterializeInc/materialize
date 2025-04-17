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
ALTER SYSTEM SET pg_source_wal_sender_timeout = ''
ALTER SYSTEM SET pg_source_tcp_configure_server = FALSE

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg_conn CASCADE;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER sconn_drop_cluster SIZE '${arg.default-replica-size}'

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE table_1 (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO table_1 VALUES (1, 'one');
ALTER TABLE table_1 REPLICA IDENTITY FULL;
INSERT INTO table_1 VALUES (2, 'two');
INSERT INTO table_1 VALUES (3, 'three');

> CREATE SOURCE mz_source
  IN CLUSTER sconn_drop_cluster
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source');

> CREATE TABLE table_1 FROM SOURCE mz_source (REFERENCE table_1);

> SELECT * FROM table_1;
1 one
2 two
3 three

# disable cluster
> ALTER CLUSTER sconn_drop_cluster SET (REPLICATION FACTOR 0)
