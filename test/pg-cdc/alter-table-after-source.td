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