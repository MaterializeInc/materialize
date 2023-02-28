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

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )


#
# Add column

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE add_columns (f1 INTEGER);
ALTER TABLE add_columns REPLICA IDENTITY FULL;
INSERT INTO add_columns VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM add_columns;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE add_columns ADD COLUMN f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * from add_columns;
1
2

> DROP SOURCE mz_source;


#
# Remove column

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE remove_column REPLICA IDENTITY FULL;
INSERT INTO remove_column VALUES (2, 'ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from remove_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE remove_column DROP COLUMN f2;
INSERT INTO remove_column VALUES (3);

! SELECT * from remove_column;
contains:altered

> DROP SOURCE mz_source;


#
# Alter column type

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE alter_column REPLICA IDENTITY FULL;
INSERT INTO alter_column VALUES (2, 'ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_column ALTER COLUMN f2 TYPE CHAR(2);
INSERT INTO alter_column VALUES (3, 'bc');

! SELECT * from alter_column;
contains:altered

> DROP SOURCE mz_source;


#
# Drop NOT NULL

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_drop_nullability (f1 INTEGER NOT NULL);
ALTER TABLE alter_drop_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_drop_nullability VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_drop_nullability
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_nullability ALTER COLUMN f1 DROP NOT NULL;
INSERT INTO alter_drop_nullability VALUES (NULL);

! SELECT * FROM alter_drop_nullability WHERE f1 IS NOT NULL;
contains:altered

# We have guaranteed that this column is not null so the optimizer eagerly
# returns the empty set.
> SELECT * FROM alter_drop_nullability WHERE f1 IS NULL;

> DROP SOURCE mz_source;


#
# Add NOT NULL

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_add_nullability (f1 INTEGER);
ALTER TABLE alter_add_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_add_nullability VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_add_nullability
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_nullability ALTER COLUMN f1 SET NOT NULL;
INSERT INTO alter_add_nullability VALUES (1);

> SELECT * FROM alter_add_nullability;
1
1

> DROP SOURCE mz_source;


#
# Drop PK

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_drop_pk (f1 INTEGER PRIMARY KEY);
ALTER TABLE alter_drop_pk REPLICA IDENTITY FULL;
INSERT INTO alter_drop_pk VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_drop_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_pk DROP CONSTRAINT alter_drop_pk_pkey;
INSERT INTO alter_drop_pk VALUES (1);

! SELECT f1 FROM alter_drop_pk;
contains:altered

> DROP SOURCE mz_source;


#
# Add PK

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_add_pk (f1 INTEGER);
ALTER TABLE alter_add_pk REPLICA IDENTITY FULL;
INSERT INTO alter_add_pk VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_add_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_add_pk VALUES (2);

> SELECT * FROM alter_add_pk;
1
2

> DROP SOURCE mz_source;


#
# Cycle PK

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_cycle_pk (f1 INTEGER PRIMARY KEY);
ALTER TABLE alter_cycle_pk REPLICA IDENTITY FULL;
INSERT INTO alter_cycle_pk VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_cycle_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_cycle_pk DROP CONSTRAINT alter_cycle_pk_pkey;
ALTER TABLE alter_cycle_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_cycle_pk VALUES (2);

! SELECT * FROM alter_cycle_pk;
contains:altered

> DROP SOURCE mz_source;


#
# Cycle PK off (no pk, pk, no pk)

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_cycle_pk_off (f1 INTEGER);
ALTER TABLE alter_cycle_pk_off REPLICA IDENTITY FULL;
INSERT INTO alter_cycle_pk_off VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_cycle_pk_off
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_cycle_pk_off ADD PRIMARY KEY(f1);
ALTER TABLE alter_cycle_pk_off DROP CONSTRAINT alter_cycle_pk_off_pkey;
INSERT INTO alter_cycle_pk_off VALUES (1);

> SELECT * FROM alter_cycle_pk_off;
1
1

> DROP SOURCE mz_source;


#
# Drop unique

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_drop_unique (f1 INTEGER UNIQUE);
ALTER TABLE alter_drop_unique REPLICA IDENTITY FULL;
INSERT INTO alter_drop_unique VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_drop_unique
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_unique DROP CONSTRAINT alter_drop_unique_f1_key;
INSERT INTO alter_drop_unique VALUES (1);

! SELECT f1 FROM alter_drop_unique;
contains:altered

> DROP SOURCE mz_source;


#
# Add unique

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_add_unique (f1 INTEGER);
ALTER TABLE alter_add_unique REPLICA IDENTITY FULL;
INSERT INTO alter_add_unique VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_add_unique
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_unique ADD UNIQUE(f1);
INSERT INTO alter_add_unique VALUES (2);

> SELECT * FROM alter_add_unique;
1
2

> DROP SOURCE mz_source;


#
# Extend column

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
ALTER TABLE alter_extend_column REPLICA IDENTITY FULL;
INSERT INTO alter_extend_column VALUES ('ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_extend_column
ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_extend_column ALTER COLUMN f1 TYPE VARCHAR(20);
INSERT INTO alter_extend_column VALUES ('abcd');

! SELECT * FROM alter_extend_column;
contains:altered

> DROP SOURCE mz_source;


#
# Alter decimal

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
ALTER TABLE alter_decimal REPLICA IDENTITY FULL;
INSERT INTO alter_decimal VALUES (123.45);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_decimal
123.45

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_decimal ALTER COLUMN f1 TYPE DECIMAL(6,1);
INSERT INTO alter_decimal VALUES (12345.6);

! SELECT * FROM alter_decimal;
contains:altered

> DROP SOURCE mz_source;


#
# Alter set with OIDs

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_set_with_oids (f1 INTEGER) WITHOUT OIDS;
ALTER TABLE alter_set_with_oids REPLICA IDENTITY FULL;
INSERT INTO alter_set_with_oids VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_set_with_oids
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_set_with_oids SET WITH OIDS;
INSERT INTO alter_set_with_oids VALUES (2);

> SELECT * FROM alter_set_with_oids;
1
2

> DROP SOURCE mz_source;


#
# Alter set without OIDs

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_set_without_oids (f1 INTEGER) WITH OIDS;
ALTER TABLE alter_set_without_oids REPLICA IDENTITY FULL;
INSERT INTO alter_set_without_oids VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_set_without_oids
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_set_without_oids SET WITHOUT OIDS;
INSERT INTO alter_set_without_oids VALUES (2);

> SELECT * FROM alter_set_without_oids;
1
2

> DROP SOURCE mz_source;


#
# Alter table rename

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_rename (f1 INTEGER);
ALTER TABLE alter_table_rename REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * from alter_table_rename;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;
INSERT INTO alter_table_renamed VALUES (2);

! SELECT * FROM alter_table_rename;
contains:altered

> DROP SOURCE mz_source;
> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (alter_table_renamed);

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO alter_table_renamed VALUES (3);

> SELECT * FROM alter_table_renamed;
1
2
3

> DROP SOURCE mz_source;


#
# Alter table rename colum

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
ALTER TABLE alter_table_rename_column REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM alter_table_rename_column;
f1_orig f2_orig

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_rename_column RENAME COLUMN f1 TO f3;
ALTER TABLE alter_table_rename_column RENAME COLUMN f2 TO f1;
ALTER TABLE alter_table_rename_column RENAME COLUMN f3 TO f2;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_renamed', 'f2_renamed');

! SELECT * FROM alter_table_rename_column;
contains:altered

> DROP SOURCE mz_source;


#
# Change column attnum

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_change_attnum (f1 VARCHAR(10), f2 VARCHAR(10));
ALTER TABLE alter_table_change_attnum REPLICA IDENTITY FULL;
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES ("alter_table_change_attnum") WITH (size = '1');

> SELECT * from alter_table_change_attnum;
f1_orig f2_orig

# Ensure simpl name swap doesn't fool schema detection
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_change_attnum DROP COLUMN f2;
ALTER TABLE alter_table_change_attnum ADD COLUMN f2 VARCHAR(10);
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_changed', 'f2_changed');

! SELECT * FROM alter_table_change_attnum;
contains:altered

> DROP SOURCE mz_source;


$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_supported (f1 int, f2 int);
ALTER TABLE alter_table_supported REPLICA IDENTITY FULL;
INSERT INTO alter_table_supported (f1, f2) VALUES (1, 1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES ("alter_table_supported") WITH (size = '1');

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
INSERT INTO alter_table_supported (f1) VALUES (1);

! SELECT * from alter_table_supported;
contains:altered

> DROP SOURCE mz_source;
