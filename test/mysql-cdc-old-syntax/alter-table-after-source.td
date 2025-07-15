# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s

#
# Test ALTER TABLE -- source will error out for tables which existed when the source was created
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE add_columns (f1 INTEGER);
INSERT INTO add_columns VALUES (1);

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO remove_column VALUES (2, 'ab');

CREATE TABLE alter_column_type_1 (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO alter_column_type_1 VALUES (2, 'ab');

CREATE TABLE alter_column_type_2 (f1 TINYINT);
INSERT INTO alter_column_type_2 VALUES (2);

CREATE TABLE alter_drop_nullability (f1 INTEGER NOT NULL);
INSERT INTO alter_drop_nullability VALUES (1);

CREATE TABLE alter_add_nullability (f1 INTEGER);
INSERT INTO alter_add_nullability VALUES (1);

CREATE TABLE alter_drop_pk (f1 INTEGER PRIMARY KEY);
INSERT INTO alter_drop_pk VALUES (1);

CREATE TABLE alter_add_pk (f1 INTEGER);
INSERT INTO alter_add_pk VALUES (1);

CREATE TABLE alter_cycle_pk (f1 INTEGER PRIMARY KEY);
INSERT INTO alter_cycle_pk VALUES (1);

CREATE TABLE alter_cycle_pk_off (f1 INTEGER);
INSERT INTO alter_cycle_pk_off VALUES (1);

CREATE TABLE alter_drop_unique (f1 INTEGER UNIQUE);
INSERT INTO alter_drop_unique VALUES (1);

CREATE TABLE alter_add_unique (f1 INTEGER);
INSERT INTO alter_add_unique VALUES (1);

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
INSERT INTO alter_extend_column VALUES ('ab');

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
INSERT INTO alter_decimal VALUES (123.45);

CREATE TABLE alter_table_rename (f1 INTEGER);
INSERT INTO alter_table_rename VALUES (1);

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_change_attnum (f1 VARCHAR(10), f2 VARCHAR(10));
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_supported (f1 int, f2 int);
INSERT INTO alter_table_supported (f1, f2) VALUES (1, 1);

CREATE TABLE truncate_table (f1 int, f2 int);
INSERT INTO truncate_table (f1, f2) VALUES (1, 1);

CREATE TABLE drop_table (f1 int, f2 int);
INSERT INTO drop_table (f1, f2) VALUES (1, 1);

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

#
# Add column

> SELECT * FROM add_columns;
1

$ mysql-execute name=mysql
ALTER TABLE add_columns ADD COLUMN f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * from add_columns;
1
2


#
# Remove column

> SELECT * FROM remove_column;
2 ab

$ mysql-execute name=mysql
ALTER TABLE remove_column DROP COLUMN f2;
INSERT INTO remove_column VALUES (3);

! select * from remove_column;
contains:incompatible schema change


#
# Alter column type

> SELECT * from alter_column_type_1;
2 ab

$ mysql-execute name=mysql
UPDATE alter_column_type_1 SET f2 = '12';
ALTER TABLE alter_column_type_1 MODIFY f2 int;
INSERT INTO alter_column_type_1 VALUES (3, 23);

! select * from alter_column_type_1;
contains:incompatible schema change

> SELECT * from alter_column_type_2;
2

$ mysql-execute name=mysql
ALTER TABLE alter_column_type_2 MODIFY f1 int;
INSERT INTO alter_column_type_2 VALUES (2048);

! select * from alter_column_type_2;
contains:incompatible schema change

#
# Drop NOT NULL

> SELECT * from alter_drop_nullability
1

$ mysql-execute name=mysql
ALTER TABLE alter_drop_nullability MODIFY f1 INTEGER;
INSERT INTO alter_drop_nullability VALUES (NULL);

! SELECT * FROM alter_drop_nullability WHERE f1 IS NOT NULL;
contains:incompatible schema change

# We have guaranteed that this column is not null so the optimizer eagerly
# returns the empty set.
> SELECT * FROM alter_drop_nullability WHERE f1 IS NULL;


#
# Add NOT NULL

> SELECT * from alter_add_nullability
1

$ mysql-execute name=mysql
ALTER TABLE alter_add_nullability MODIFY f1 INTEGER NOT NULL;
INSERT INTO alter_add_nullability VALUES (1);

> select * from alter_add_nullability;
1
1

# TODO: database-issues#7475 (changes to columns)
# ? EXPLAIN SELECT * FROM alter_add_nullability WHERE f1 IS NULL;
# Explained Query (fast path):
#   Constant <empty>

#
# Drop PK

> SELECT * from alter_drop_pk
1

$ mysql-execute name=mysql
ALTER TABLE alter_drop_pk DROP PRIMARY KEY;
INSERT INTO alter_drop_pk VALUES (1);

! SELECT f1 FROM alter_drop_pk;
contains:incompatible schema change


#
# Add PK

> SELECT * from alter_add_pk
1

$ mysql-execute name=mysql
ALTER TABLE alter_add_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_add_pk VALUES (2);

> SELECT * FROM alter_add_pk;
1
2


#
# Cycle PK

> SELECT * from alter_cycle_pk
1

$ mysql-execute name=mysql
ALTER TABLE alter_cycle_pk DROP PRIMARY KEY;
ALTER TABLE alter_cycle_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_cycle_pk VALUES (2);

! SELECT * FROM alter_cycle_pk;
contains:incompatible schema change

#
# Cycle PK off (no pk, pk, no pk)


> SELECT * from alter_cycle_pk_off
1

$ mysql-execute name=mysql
ALTER TABLE alter_cycle_pk_off ADD PRIMARY KEY(f1);
ALTER TABLE alter_cycle_pk_off DROP PRIMARY KEY;
INSERT INTO alter_cycle_pk_off VALUES (1);

> SELECT * FROM alter_cycle_pk_off;
1
1

#
# Drop unique

> SELECT * from alter_drop_unique
1

$ mysql-execute name=mysql
ALTER TABLE alter_drop_unique DROP INDEX f1;
INSERT INTO alter_drop_unique VALUES (1);

! SELECT f1 FROM alter_drop_unique;
contains:incompatible schema change


#
# Add unique
> SELECT * from alter_add_unique
1

$ mysql-execute name=mysql
ALTER TABLE alter_add_unique MODIFY f1 INTEGER UNIQUE;
INSERT INTO alter_add_unique VALUES (2);

> SELECT * FROM alter_add_unique;
1
2


#
# Extend column


> SELECT * from alter_extend_column
ab

$ mysql-execute name=mysql
ALTER TABLE alter_extend_column MODIFY f1 VARCHAR(20);
INSERT INTO alter_extend_column VALUES ('abcd');

! SELECT * FROM alter_extend_column;
contains:incompatible schema change

#
# Alter decimal
> SELECT * from alter_decimal
123.45

$ mysql-execute name=mysql
ALTER TABLE alter_decimal MODIFY COLUMN f1 DECIMAL(6,1);
INSERT INTO alter_decimal VALUES (12345.6);

! SELECT * FROM alter_decimal;
contains:altered


#
# Alter table rename

> SELECT * from alter_table_rename;
1

$ mysql-execute name=mysql
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;
INSERT INTO alter_table_renamed VALUES (2);

! SELECT * FROM alter_table_rename;
contains:table was dropped

$ mysql-execute name=mysql
INSERT INTO alter_table_renamed VALUES (3);

! SELECT * FROM alter_table_renamed;
contains:unknown


#
# Alter table rename colum

> SELECT * FROM alter_table_rename_column;
f1_orig f2_orig

$ mysql-execute name=mysql
# use CHANGE instead of RENAME COLUMN f1 TO f3 to be compatible with MySQL 5.7
ALTER TABLE alter_table_rename_column CHANGE f1 f3 varchar(10);
ALTER TABLE alter_table_rename_column CHANGE f2 f1 varchar(10);
ALTER TABLE alter_table_rename_column CHANGE f3 f2 varchar(10);
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_renamed', 'f2_renamed');

! SELECT * FROM alter_table_rename_column;
contains:incompatible schema change


# Change column attnum

> SELECT * from alter_table_change_attnum;
f1_orig f2_orig

# Ensure simpl name swap doesn't fool schema detection
$ mysql-execute name=mysql
ALTER TABLE alter_table_change_attnum DROP COLUMN f2;
ALTER TABLE alter_table_change_attnum ADD COLUMN f2 VARCHAR(10);
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_changed', 'f2_changed');

! SELECT * FROM alter_table_change_attnum;
contains:incompatible schema change

> SELECT * from alter_table_supported;
1 1

$ mysql-execute name=mysql
ALTER TABLE alter_table_supported ADD COLUMN f3 int;
INSERT INTO alter_table_supported (f1, f2, f3) VALUES (2, 2, 2);

> SELECT * from alter_table_supported;
1 1
2 2

$ mysql-execute name=mysql
ALTER TABLE alter_table_supported DROP COLUMN f3;
INSERT INTO alter_table_supported (f1, f2) VALUES (3, 3);

> SELECT * from alter_table_supported;
1 1
2 2
3 3

$ mysql-execute name=mysql
ALTER TABLE alter_table_supported DROP COLUMN f2;
INSERT INTO alter_table_supported (f1) VALUES (1);

! SELECT * from alter_table_supported;
contains:incompatible schema change


#
# Truncate table

> SELECT * from truncate_table;
1 1

$ mysql-execute name=mysql
TRUNCATE truncate_table;

! SELECT * FROM truncate_table;
contains: table was truncated

#
# Drop table

> SELECT * from drop_table;
1 1

$ mysql-execute name=mysql
DROP TABLE drop_table;

# TODO: redesign ceased status database-issues#7687
# > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'drop_table';
# ceased

# this should not brick the whole source
> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

# the subsources are in a bad state so we need to drop the source so the
# restart check doesn't error
> DROP SOURCE mz_source CASCADE;
