# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-532: dropping and recreating a constraint or a table
# upstream with the same shape as before.
#
# Constraint changes are not recorded in `cdc.ddl_history`, so the source
# re-reads each table's PRIMARY KEY and UNIQUE constraints on every poll that
# sees new changes and compares them by name, type, and column set. A drop
# and re-add that completes between two polls is therefore invisible, and
# rows written in the window that violated the key are ingested as if the
# key held at those timestamps. `sys.key_constraints` exposes `object_id`
# and `create_date`, which would tell the recreated constraint apart.
#
# Dropping the table makes SQL Server disable CDC on it: the capture
# instance and its change function are gone. The source keeps polling the
# missing function, fails with a transient error, and restarts every few
# seconds. Nothing is reported against the dropped table, and no table of the
# source advances until the Materialize table is dropped.
#
# This file asserts the current behavior. Each outcome marked SS-532 is the
# behavior the issue wants changed.

$ set-sql-timeout duration=60s

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

CREATE TABLE recreate_uq (id INT NOT NULL CONSTRAINT recreate_uq_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT recreate_uq_wallet UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'recreate_uq', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO recreate_uq VALUES (1, 'a');

CREATE TABLE recreate_tbl (id INT NOT NULL CONSTRAINT recreate_tbl_pk PRIMARY KEY, val VARCHAR(64));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'recreate_tbl', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO recreate_tbl VALUES (1, 'a');

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE recreate_uq FROM SOURCE sql_server_src (REFERENCE dbo.recreate_uq);
> CREATE TABLE recreate_tbl FROM SOURCE sql_server_src (REFERENCE dbo.recreate_tbl);

> SELECT * FROM recreate_uq;
1 a

> SELECT * FROM recreate_tbl;
1 a

# Materialize planned with the key, so a DISTINCT over it is free.
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT wallet FROM recreate_uq;
Explained Query:
  Project (#1)
    ReadStorage materialize.public.recreate_uq

Source materialize.public.recreate_uq

Target cluster: quickstart

#
# Drop a UNIQUE constraint, write and remove a row that violates it, then
# re-add it under the same name over the same columns. All of it commits in
# one transaction, so no poll can observe the intermediate state.
#

$ sql-server-execute name=sql-server split-lines=false
BEGIN TRANSACTION;
ALTER TABLE recreate_uq DROP CONSTRAINT recreate_uq_wallet;
INSERT INTO recreate_uq VALUES (2, 'a');
DELETE FROM recreate_uq WHERE id = 2;
ALTER TABLE recreate_uq ADD CONSTRAINT recreate_uq_wallet UNIQUE (wallet);
INSERT INTO recreate_uq VALUES (3, 'b');
COMMIT;

# SS-532: the drop and the re-add are invisible. The table keeps ingesting,
# and at the timestamps between the insert and the delete of id 2 it held
# two rows with wallet 'a' under a schema that promised wallet is unique.
> SELECT * FROM recreate_uq;
1 a
3 b

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'recreate_uq';
running

#
# Drop and recreate a table with the same definition.
#

$ sql-server-execute name=sql-server
DROP TABLE recreate_tbl;
CREATE TABLE recreate_tbl (id INT NOT NULL CONSTRAINT recreate_tbl_pk PRIMARY KEY, val VARCHAR(64));
INSERT INTO recreate_tbl VALUES (1, 'a'), (2, 'b');
INSERT INTO recreate_uq VALUES (4, 'd');

# SS-532: the drop is a transient error on the source, not a definite error
# on the table. The source restarts every few seconds and never gets past it.
> SELECT count(*) > 0 FROM mz_internal.mz_source_status_history h
  JOIN mz_sources s ON s.id = h.source_id
  WHERE s.name = 'sql_server_src' AND h.status = 'stalled'
  AND h.error LIKE '%Invalid object name ''cdc.fn_cdc_get_all_changes_dbo_recreate_tbl''%'
true

# SS-532: the restart loop also stops recreate_uq, which was not touched. Its
# write frontier stays put, and the row inserted after the drop never arrives.
# Reads of either table are not asserted here: a strict serializable read
# blocks once the chosen timestamp passes the stuck frontier.
$ set-from-sql var=stuck-frontier
SELECT write_frontier::text FROM mz_internal.mz_frontiers f JOIN mz_tables t ON t.id = f.object_id WHERE t.name = 'recreate_uq'

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

> SELECT write_frontier::text = '${stuck-frontier}'
  FROM mz_internal.mz_frontiers f JOIN mz_tables t ON t.id = f.object_id
  WHERE t.name = 'recreate_uq'
true

# Re-enabling CDC on the recreated table under the old capture instance name
# does not recover the source either: it resumes from an LSN older than the
# new capture instance's start LSN.
$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'recreate_tbl', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'dbo_recreate_tbl';
INSERT INTO recreate_tbl VALUES (3, 'c');

> SELECT count(*) > 0 FROM mz_internal.mz_source_status_history h
  JOIN mz_sources s ON s.id = h.source_id
  WHERE s.name = 'sql_server_src' AND h.status = 'stalled'
  AND h.error LIKE '%is less than the minimum%dbo_recreate_tbl%'
true

# The only recovery is to drop the Materialize table, which takes the capture
# instance out of the source and lets the other tables resume, and then to
# create a new table, which re-snapshots.
> DROP TABLE recreate_tbl;

> SELECT * FROM recreate_uq;
1 a
3 b
4 d

> CREATE TABLE recreate_tbl_v2 FROM SOURCE sql_server_src (REFERENCE dbo.recreate_tbl);

> SELECT * FROM recreate_tbl_v2;
1 a
2 b
3 c

> DROP SOURCE sql_server_src CASCADE;
