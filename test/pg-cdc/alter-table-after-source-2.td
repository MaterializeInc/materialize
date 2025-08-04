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

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET pg_schema_validation_interval = '2s';

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE add_columns (f1 INTEGER);
ALTER TABLE add_columns REPLICA IDENTITY FULL;
INSERT INTO add_columns VALUES (1);

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE remove_column REPLICA IDENTITY FULL;
INSERT INTO remove_column VALUES (2, 'ab');

CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE alter_column REPLICA IDENTITY FULL;
INSERT INTO alter_column VALUES (2, 'ab');

CREATE TABLE alter_drop_nullability (f1 INTEGER NOT NULL);
ALTER TABLE alter_drop_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_drop_nullability VALUES (1);

CREATE TABLE alter_add_nullability (f1 INTEGER);
ALTER TABLE alter_add_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_add_nullability VALUES (1);

CREATE TABLE alter_drop_pk (f1 INTEGER PRIMARY KEY);
ALTER TABLE alter_drop_pk REPLICA IDENTITY FULL;
INSERT INTO alter_drop_pk VALUES (1);

CREATE TABLE alter_add_pk (f1 INTEGER);
ALTER TABLE alter_add_pk REPLICA IDENTITY FULL;
INSERT INTO alter_add_pk VALUES (1);

CREATE TABLE alter_cycle_pk (f1 INTEGER PRIMARY KEY);
ALTER TABLE alter_cycle_pk REPLICA IDENTITY FULL;
INSERT INTO alter_cycle_pk VALUES (1);

CREATE TABLE alter_cycle_pk_off (f1 INTEGER);
ALTER TABLE alter_cycle_pk_off REPLICA IDENTITY FULL;
INSERT INTO alter_cycle_pk_off VALUES (1);

CREATE TABLE alter_drop_unique (f1 INTEGER UNIQUE);
ALTER TABLE alter_drop_unique REPLICA IDENTITY FULL;
INSERT INTO alter_drop_unique VALUES (1);

CREATE TABLE alter_add_unique (f1 INTEGER);
ALTER TABLE alter_add_unique REPLICA IDENTITY FULL;
INSERT INTO alter_add_unique VALUES (1);

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
ALTER TABLE alter_extend_column REPLICA IDENTITY FULL;
INSERT INTO alter_extend_column VALUES ('ab');

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
ALTER TABLE alter_decimal REPLICA IDENTITY FULL;
INSERT INTO alter_decimal VALUES (123.45);

CREATE TABLE alter_table_rename (f1 INTEGER);
ALTER TABLE alter_table_rename REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename VALUES (1);

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
ALTER TABLE alter_table_rename_column REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_change_attnum (f1 VARCHAR(10), f2 VARCHAR(10));
ALTER TABLE alter_table_change_attnum REPLICA IDENTITY FULL;
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_supported (f1 int, f2 int);
ALTER TABLE alter_table_supported REPLICA IDENTITY FULL;
INSERT INTO alter_table_supported (f1, f2) VALUES (1, 1);

CREATE TABLE truncate_table (f1 int, f2 int);
ALTER TABLE truncate_table REPLICA IDENTITY FULL;
INSERT INTO truncate_table (f1, f2) VALUES (1, 1);

CREATE TABLE drop_table (f1 int, f2 int);
ALTER TABLE drop_table REPLICA IDENTITY FULL;
INSERT INTO drop_table (f1, f2) VALUES (1, 1);

CREATE PUBLICATION mz_source FOR ALL TABLES;


> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE add_columns FROM SOURCE mz_source (REFERENCE add_columns);
> CREATE TABLE remove_column FROM SOURCE mz_source (REFERENCE remove_column);
> CREATE TABLE alter_column FROM SOURCE mz_source (REFERENCE alter_column);
> CREATE TABLE alter_drop_nullability FROM SOURCE mz_source (REFERENCE alter_drop_nullability);
> CREATE TABLE alter_add_nullability FROM SOURCE mz_source (REFERENCE alter_add_nullability);
> CREATE TABLE alter_drop_pk FROM SOURCE mz_source (REFERENCE alter_drop_pk);
> CREATE TABLE alter_add_pk FROM SOURCE mz_source (REFERENCE alter_add_pk);
> CREATE TABLE alter_cycle_pk FROM SOURCE mz_source (REFERENCE alter_cycle_pk);
> CREATE TABLE alter_cycle_pk_off FROM SOURCE mz_source (REFERENCE alter_cycle_pk_off);
> CREATE TABLE alter_drop_unique FROM SOURCE mz_source (REFERENCE alter_drop_unique);
> CREATE TABLE alter_add_unique FROM SOURCE mz_source (REFERENCE alter_add_unique);
> CREATE TABLE alter_extend_column FROM SOURCE mz_source (REFERENCE alter_extend_column);
> CREATE TABLE alter_decimal FROM SOURCE mz_source (REFERENCE alter_decimal);
> CREATE TABLE alter_table_rename FROM SOURCE mz_source (REFERENCE alter_table_rename);
> CREATE TABLE alter_table_rename_column FROM SOURCE mz_source (REFERENCE alter_table_rename_column);
> CREATE TABLE alter_table_change_attnum FROM SOURCE mz_source (REFERENCE alter_table_change_attnum);
> CREATE TABLE alter_table_supported FROM SOURCE mz_source (REFERENCE alter_table_supported);
> CREATE TABLE truncate_table FROM SOURCE mz_source (REFERENCE truncate_table);
> CREATE TABLE drop_table FROM SOURCE mz_source (REFERENCE drop_table);


#
# Alter table rename

> SELECT * from alter_table_rename;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;

! SELECT * FROM alter_table_rename;
contains:altered

#
# Alter table rename colum

> SELECT * FROM alter_table_rename_column;
f1_orig f2_orig

$ postgres-execute connection=postgres://postgres:postgres@postgres
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
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_change_attnum DROP COLUMN f2;
ALTER TABLE alter_table_change_attnum ADD COLUMN f2 VARCHAR(10);

! SELECT * FROM alter_table_change_attnum;
contains:altered

> SELECT * from alter_table_supported;
1 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_supported ADD COLUMN f3 int;
INSERT INTO alter_table_supported (f1, f2, f3) VALUES (2, 2, 2);

> SELECT * from alter_table_supported;
1 1
2 2

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_supported DROP COLUMN f3;
INSERT INTO alter_table_supported (f1, f2) VALUES (3, 3);

> SELECT * from alter_table_supported;
1 1
2 2
3 3

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_supported DROP COLUMN f2;

! SELECT * from alter_table_supported;
contains:altered


#
# Truncate table

> SELECT * from truncate_table;
1 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE truncate_table;

! SELECT * FROM truncate_table;
contains:table was truncated

#
# Drop table

> SELECT * from drop_table;
1 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE drop_table;

# Table is dropped
! SELECT * FROM drop_table;
regex:(table was dropped|incompatible schema change)
