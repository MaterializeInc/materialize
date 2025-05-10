# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test ALTER TABLE -- source will error out for tables which existed when the source was created
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

$ sql-server-execute name=sql-server
CREATE TABLE add_columns (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'add_columns', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO add_columns VALUES (1);

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'remove_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO remove_column VALUES (2, 'ab');

CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_column VALUES (2, 'ab');

CREATE TABLE alter_drop_nullability (f1 INTEGER NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_drop_nullability', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_drop_nullability VALUES (1);

CREATE TABLE alter_add_nullability (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_nullability', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_nullability VALUES (1);

CREATE TABLE alter_drop_pk (f1 INTEGER PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_drop_pk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_drop_pk VALUES (1);

CREATE TABLE alter_add_pk (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_pk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_pk VALUES (1);

CREATE TABLE alter_cycle_pk (f1 INTEGER PRIMARY KEY);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_cycle_pk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_cycle_pk VALUES (1);

CREATE TABLE alter_cycle_pk_off (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_cycle_pk_off', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_cycle_pk_off VALUES (1);

CREATE TABLE alter_drop_unique (f1 INTEGER UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_drop_unique', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_drop_unique VALUES (1);

CREATE TABLE alter_add_unique (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_unique', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_unique VALUES (1);

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_extend_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_extend_column VALUES ('ab');

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_decimal', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_decimal VALUES (123.45);

CREATE TABLE alter_table_rename (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_rename', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_rename VALUES (1);

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_rename_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_change_attnum (f1 VARCHAR(10), f2 VARCHAR(10));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_change_attnum', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_supported (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_supported', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_supported (f1, f2) VALUES (1, 1);

CREATE TABLE truncate_table (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'truncate_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO truncate_table (f1, f2) VALUES (1, 1);

CREATE TABLE drop_table (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'drop_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO drop_table (f1, f2) VALUES (1, 1);


> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

#
# Add column

> SELECT * FROM add_columns;
1

$ sql-server-execute name=sql-server
ALTER TABLE add_columns ADD f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * from add_columns;
1
2


#
# Remove column

> SELECT * from remove_column;
2 ab

$ sql-server-execute name=sql-server
ALTER TABLE remove_column DROP COLUMN f2;

# TODO: This should fail
# ! SELECT * from remove_column;
# contains:altered


#
# Alter column type

> SELECT * from alter_column;
2 ab

$ sql-server-execute name=sql-server
ALTER TABLE alter_column ALTER COLUMN f2 CHAR(2);

# TODO: This should fail
# ! SELECT * from alter_column;
# contains:altered


#
# Drop NOT NULL

> SELECT * from alter_drop_nullability
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_drop_nullability ALTER COLUMN f1 INTEGER NULL;

! SELECT * FROM alter_drop_nullability WHERE f1 IS NOT NULL;
contains:altered

# We have guaranteed that this column is not null so the optimizer eagerly
# returns the empty set.
> SELECT * FROM alter_drop_nullability WHERE f1 IS NULL;


#
# Add NOT NULL

> SELECT * from alter_add_nullability
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_nullability ALTER COLUMN f1 SET NOT NULL;
INSERT INTO alter_add_nullability VALUES (1);

> SELECT * FROM alter_add_nullability;
1
1


#
# Drop PK

> SELECT * from alter_drop_pk
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_drop_pk DROP CONSTRAINT alter_drop_pk_pkey;

! SELECT f1 FROM alter_drop_pk;
contains:altered


#
# Add PK

> SELECT * from alter_add_pk
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_add_pk VALUES (2);

> SELECT * FROM alter_add_pk;
1
2


#
# Cycle PK

> SELECT * from alter_cycle_pk
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_cycle_pk DROP CONSTRAINT alter_cycle_pk_pkey;
ALTER TABLE alter_cycle_pk ADD PRIMARY KEY(f1);

! SELECT * FROM alter_cycle_pk;
contains:altered


#
# Cycle PK off (no pk, pk, no pk)


> SELECT * from alter_cycle_pk_off
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_cycle_pk_off ADD PRIMARY KEY(f1);
ALTER TABLE alter_cycle_pk_off DROP CONSTRAINT alter_cycle_pk_off_pkey;
INSERT INTO alter_cycle_pk_off VALUES (1);

> SELECT * FROM alter_cycle_pk_off;
1
1


#
# Drop unique

> SELECT * from alter_drop_unique
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_drop_unique DROP CONSTRAINT alter_drop_unique_f1_key;

! SELECT f1 FROM alter_drop_unique;
contains:altered


#
# Add unique
> SELECT * from alter_add_unique
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_unique ADD UNIQUE(f1);
INSERT INTO alter_add_unique VALUES (2);

> SELECT * FROM alter_add_unique;
1
2


#
# Extend column


> SELECT * from alter_extend_column
ab

$ sql-server-execute name=sql-server
ALTER TABLE alter_extend_column ALTER COLUMN f1 TYPE VARCHAR(20);

! SELECT * FROM alter_extend_column;
contains:altered


#
# Alter decimal
> SELECT * from alter_decimal
123.45

$ sql-server-execute name=sql-server
ALTER TABLE alter_decimal ALTER COLUMN f1 TYPE DECIMAL(6,1);

! SELECT * FROM alter_decimal;
contains:altered


#
# Alter table rename

> SELECT * from alter_table_rename;
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;

! SELECT * FROM alter_table_rename;
contains:altered

#
# Alter table rename column

> SELECT * FROM alter_table_rename_column;
f1_orig f2_orig

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_rename_column RENAME COLUMN f1 TO f3;
ALTER TABLE alter_table_rename_column RENAME COLUMN f2 TO f1;
ALTER TABLE alter_table_rename_column RENAME COLUMN f3 TO f2;

! SELECT * FROM alter_table_rename_column;
contains:altered


#
# Change column attnum

> SELECT * from alter_table_change_attnum;
f1_orig f2_orig

# Ensure simpl name swap doesn't fool schema detection
$ sql-server-execute name=sql-server
ALTER TABLE alter_table_change_attnum DROP COLUMN f2;
ALTER TABLE alter_table_change_attnum ADD f2 VARCHAR(10);

! SELECT * FROM alter_table_change_attnum;
contains:altered

> SELECT * from alter_table_supported;
1 1

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported ADD f3 int;
INSERT INTO alter_table_supported (f1, f2, f3) VALUES (2, 2, 2);

> SELECT * from alter_table_supported;
1 1
2 2

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported DROP COLUMN f3;
INSERT INTO alter_table_supported (f1, f2) VALUES (3, 3);

> SELECT * from alter_table_supported;
1 1
2 2
3 3

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported DROP COLUMN f2;

! SELECT * from alter_table_supported;
contains:altered


#
# Truncate table

> SELECT * from truncate_table;
1 1

$ sql-server-execute name=sql-server
TRUNCATE truncate_table;

! SELECT * FROM truncate_table;
contains:table was truncated

#
# Drop table

> SELECT * from drop_table;
1 1

$ sql-server-execute name=sql-server
DROP TABLE drop_table;

# Table is dropped
! SELECT * FROM drop_table;
regex:(table was dropped|incompatible schema change)
