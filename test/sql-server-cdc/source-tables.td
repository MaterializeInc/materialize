# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1_pk (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_pk', @role_name = 'SA', @capture_instance = N't1_pk_cdc1', @supports_net_changes = 0;

INSERT INTO t1_pk VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

CREATE TABLE t2_no_cdc (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));

CREATE TABLE t3_text (value VARCHAR(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3_text', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE text_cols (c1 decimal(20, 10), c2 time, c3 money, c4 varbinary(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'text_cols', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO text_cols VALUES (1.444889, '12:00:00', '$100.99', 0x1100AB);


# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

> CREATE CONNECTION ms_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

# Create a SQL Server Source.
> CREATE CLUSTER src_tables (SIZE = '1', REPLICATION FACTOR = 1);
> SET CLUSTER TO src_tables;
> CREATE SOURCE ms_src FROM SQL SERVER CONNECTION ms_conn;

# Can't create tables from SQL server source where it doesn't EXISTS
! CREATE TABLE does_not_exist FROM SOURCE ms_src (REFERENCE does_not_exist);
contains:not found in source

# Can't create tables from SQL server source where table doesn't have cdc enabled
! CREATE TABLE no_cdc FROM SOURCE ms_src (REFERENCE t2_no_cdc);
contains:not found in source

# Can create tables from SQL server source
> CREATE TABLE t1 FROM SOURCE ms_src (REFERENCE t1_pk);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SELECT * FROM t1;
a "hello world"
b "foobar"
c "anotha one"

# using a qualified name should work as well
> CREATE TABLE t2 FROM SOURCE ms_src (REFERENCE dbo.t1_pk);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SELECT * FROM t2;
a "hello world"
b "foobar"
c "anotha one"

# source tables see updates
$ sql-server-execute name=sql-server
DELETE FROM t1_pk WHERE key_col = 'a';
INSERT INTO t1_pk VALUES ('😊', 'lets see what happens');
UPDATE t1_pk SET val_col = 'beer' WHERE key_col = 'b';
INSERT INTO t2_no_cdc VALUES ('ignored', 'update');

> SELECT * FROM t1;
b "beer"
c "anotha one"
😊 "lets see what happens"

> SELECT * FROM t2;
b "beer"
c "anotha one"
😊 "lets see what happens"

> DROP TABLE t1;

# recreate the source table after dropping it
# we should see the same values
> CREATE TABLE t1 FROM SOURCE ms_src (REFERENCE t1_pk);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SELECT * FROM t1;
b "beer"
c "anotha one"
😊 "lets see what happens"

> DROP TABLE t1;
> DROP TABLE t2;

# source tables support text columns
> CREATE TABLE text_cols FROM SOURCE ms_src (REFERENCE dbo.text_cols) WITH (TEXT COLUMNS = (c1, c2, c3, c4));

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SHOW COLUMNS FROM text_cols;
c1 true text ""
c2 true text ""
c3 true text ""
c4 true text ""

> SELECT * FROM text_cols;
1.4448890000 12:00:00 100.9900 EQCr

$ sql-server-execute name=sql-server
INSERT INTO text_cols VALUES (2.000001, '01:59:32.99901', '$0.89', 0x1122AABBCC00DD), (NULL, NULL, '$99.99', NULL);

> SELECT * FROM text_cols;
1.4448890000 12:00:00 100.9900 EQCr
2.0000010000 01:59:32.999010 0.8900 ESKqu8wA3Q==
<null> <null> 99.9900 <null>

> DROP TABLE text_cols;

# source tables support exclude columns
> CREATE TABLE exclude_cols FROM SOURCE ms_src (REFERENCE dbo.t1_pk) WITH (EXCLUDE COLUMNS = (key_col));

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SHOW COLUMNS FROM exclude_cols;
val_col true "character varying" ""

> SELECT * FROM exclude_cols;
"beer"
"anotha one"
"lets see what happens"

$ sql-server-execute name=sql-server
INSERT INTO t1_pk(key_col, val_col) VALUES ('d', 'dogs'), ('e', 'elephant');

> SELECT * FROM exclude_cols;
"beer"
"anotha one"
"lets see what happens"
"dogs"
"elephant"

> DROP TABLE exclude_cols;

# Ensure that rewinds are correctly processed
> CREATE TABLE t1 FROM SOURCE ms_src (REFERENCE t1_pk);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true

> SELECT * FROM t1;
b "beer"
c "anotha one"
d "dogs"
e "elephant"
😊 "lets see what happens"

> ALTER CLUSTER src_tables SET (REPLICATION FACTOR = 0);

$ sql-server-execute name=sql-server
INSERT INTO t1_pk VALUES ('x', 'xeon'), ('y', 'yellow'), ('z', 'zion');

> CREATE TABLE t2 FROM SOURCE ms_src (REFERENCE t1_pk);

> ALTER CLUSTER src_tables SET (REPLICATION FACTOR = 1);

# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress inclusive of the table just
# created for the existing SOURCE.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'ms_src' AND ss.snapshot_records_staged > 0;
ms_src true
ms_src true

> SELECT * FROM t1;
b "beer"
c "anotha one"
d "dogs"
e "elephant"
x "xeon"
y "yellow"
z "zion"
😊 "lets see what happens"

> SELECT * FROM t2;
b "beer"
c "anotha one"
d "dogs"
e "elephant"
x "xeon"
y "yellow"
z "zion"
😊 "lets see what happens"


# create source table after adding a column to upstream table
# !!! disabled for now as we don't handle the capture instance switch gracefully.
#$ sql-server-execute name=sql-server
#ALTER TABLE t1_pk ADD extra_1 INT NULL;
#EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_pk', @capture_instance = N't1_pk_cdc2', @role_name = 'SA', @supports_net_changes = 0;
#EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 't1_pk', @capture_instance = N't1_pk_cdc1';
#
#> CREATE TABLE extra_col FROM SOURCE ms_src (REFERENCE t1_pk);
#
#> SELECT * FROM extra_col;
#b "beer" <null>
#c "anotha one" <null>
#😊 "lets see what happens" <null>
