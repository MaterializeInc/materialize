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

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE add_columns (f1 INTEGER);
ALTER TABLE add_columns REPLICA IDENTITY FULL;
INSERT INTO add_columns VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * FROM add_columns;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE add_columns ADD COLUMN f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

! SELECT * from add_columns;
contains:altered

> DROP VIEW add_columns;
> DROP SOURCE mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE remove_column REPLICA IDENTITY FULL;
INSERT INTO remove_column VALUES (2, 'ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from remove_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE remove_column DROP COLUMN f2;
INSERT INTO remove_column VALUES (3);

! SELECT * from remove_column;
contains:altered

> DROP VIEW remove_column;
> DROP SOURCE mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE alter_column REPLICA IDENTITY FULL;
INSERT INTO alter_column VALUES (2, 'ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_column;
2 ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_column ALTER COLUMN f2 TYPE CHAR(2);
INSERT INTO alter_column VALUES (3, 'bc');

! SELECT * from alter_column;
contains:altered
> DROP VIEW alter_column;
> DROP SOURCE mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_nullability (f1 INTEGER NOT NULL);
ALTER TABLE alter_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_nullability VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_nullability
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_nullability ALTER COLUMN f1 DROP NOT NULL;
INSERT INTO alter_nullability VALUES (NULL);

> SELECT * FROM alter_nullability WHERE f1 IS NOT NULL;
1

> SELECT * FROM alter_nullability WHERE f1 IS NULL;
<null>

> DROP VIEW alter_nullability;
> DROP SOURCE mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_drop_pk (f1 INTEGER PRIMARY KEY);
ALTER TABLE alter_drop_pk REPLICA IDENTITY FULL;
INSERT INTO alter_drop_pk VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_drop_pk
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_pk DROP CONSTRAINT alter_drop_pk_pkey;
INSERT INTO alter_drop_pk VALUES (1);

# Because ALTERing constraints does not show up in replication this does not cause an error
> SELECT DISTINCT f1 FROM alter_drop_pk;
1

> DROP VIEW alter_drop_pk;
> DROP SOURCE mz_source;


$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;


$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
ALTER TABLE alter_extend_column REPLICA IDENTITY FULL;
INSERT INTO alter_extend_column VALUES ('ab');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_extend_column
ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_extend_column ALTER COLUMN f1 TYPE VARCHAR(20);
INSERT INTO alter_extend_column VALUES ('abcd');

! SELECT * FROM alter_extend_column;
contains:altered

> DROP VIEW alter_extend_column;
> DROP SOURCE mz_source;



$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
ALTER TABLE alter_decimal REPLICA IDENTITY FULL;
INSERT INTO alter_decimal VALUES (123.45);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_decimal
123.45

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_decimal ALTER COLUMN f1 TYPE DECIMAL(6,1);
INSERT INTO alter_decimal VALUES (12345.6);

! SELECT * FROM alter_decimal;
contains:altered

> DROP VIEW alter_decimal;
> DROP SOURCE mz_source;



$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_set_with_oids (f1 INTEGER) WITHOUT OIDS;
ALTER TABLE alter_set_with_oids REPLICA IDENTITY FULL;
INSERT INTO alter_set_with_oids VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_set_with_oids
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_set_with_oids SET WITH OIDS;
INSERT INTO alter_set_with_oids VALUES (2);

> SELECT * FROM alter_set_with_oids;
1
2

> DROP VIEW alter_set_with_oids;
> DROP SOURCE mz_source;




$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_set_without_oids (f1 INTEGER) WITH OIDS;
ALTER TABLE alter_set_without_oids REPLICA IDENTITY FULL;
INSERT INTO alter_set_without_oids VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_set_without_oids
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_set_without_oids SET WITHOUT OIDS;
INSERT INTO alter_set_without_oids VALUES (2);

> SELECT * FROM alter_set_without_oids;
1
2

> DROP VIEW alter_set_without_oids;
> DROP SOURCE mz_source;



$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_rename (f1 INTEGER);
ALTER TABLE alter_table_rename REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_table_rename;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;
INSERT INTO alter_table_renamed VALUES (2);

! SELECT * FROM alter_table_rename;
contains:altered

! CREATE VIEWS FROM SOURCE mz_source (alter_table_renamed);
contains:alter_table_renamed not found

> DROP VIEW alter_table_rename;
> DROP SOURCE mz_source;
> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source (alter_table_renamed);

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO alter_table_renamed VALUES (3);

> SELECT * FROM alter_table_renamed;
1
2
3

> DROP VIEW alter_table_renamed;
> DROP SOURCE mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
ALTER TABLE alter_table_rename_column REPLICA IDENTITY FULL;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * from alter_table_rename_column;
f1_orig f2_orig

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_table_rename_column RENAME COLUMN f1 TO f3;
ALTER TABLE alter_table_rename_column RENAME COLUMN f2 TO f1;
ALTER TABLE alter_table_rename_column RENAME COLUMN f3 TO f2;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_renamed', 'f2_renamed');

! SELECT * FROM alter_table_rename_column;
contains:altered

> DROP VIEW alter_table_rename_column;
> DROP SOURCE mz_source;
