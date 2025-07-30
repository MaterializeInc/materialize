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
# Add column

> SELECT * FROM add_columns;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE add_columns ADD COLUMN f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * from add_columns;
1
2


#
# Remove column

> SELECT * from remove_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE remove_column DROP COLUMN f2;

! SELECT * from remove_column;
contains:altered


#
# Alter column type

> SELECT * from alter_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_column ALTER COLUMN f2 TYPE CHAR(2);

! SELECT * from alter_column;
contains:altered


#
# Drop NOT NULL

> SELECT * from alter_drop_nullability
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_nullability ALTER COLUMN f1 DROP NOT NULL;

! SELECT * FROM alter_drop_nullability WHERE f1 IS NOT NULL;
contains:altered

# We have guaranteed that this column is not null so the optimizer eagerly
# returns the empty set.
> SELECT * FROM alter_drop_nullability WHERE f1 IS NULL;


#
# Add NOT NULL

> SELECT * from alter_add_nullability
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_nullability ALTER COLUMN f1 SET NOT NULL;
INSERT INTO alter_add_nullability VALUES (1);

> SELECT * FROM alter_add_nullability;
1
1


#
# Drop PK

> SELECT * from alter_drop_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_pk DROP CONSTRAINT alter_drop_pk_pkey;

! SELECT f1 FROM alter_drop_pk;
contains:altered


#
# Add PK

> SELECT * from alter_add_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_add_pk VALUES (2);

> SELECT * FROM alter_add_pk;
1
2


#
# Cycle PK

> SELECT * from alter_cycle_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_cycle_pk DROP CONSTRAINT alter_cycle_pk_pkey;
ALTER TABLE alter_cycle_pk ADD PRIMARY KEY(f1);

! SELECT * FROM alter_cycle_pk;
contains:altered
