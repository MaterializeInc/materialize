# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

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

#
# ALTER TABLE <tbl> ADD COLUMN
#

$ mysql-execute name=mysql
CREATE TABLE add_columns (f1 INTEGER);
INSERT INTO add_columns VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.add_columns);

> SELECT * FROM add_columns;
1

$ mysql-execute name=mysql
ALTER TABLE add_columns ADD COLUMN f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * FROM add_columns;
1

#
# Now remove that added column
#

$ mysql-execute name=mysql
ALTER TABLE add_columns DROP COLUMN f2;

> SELECT * FROM add_columns;
1

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> DROP COLUMN
#

$ mysql-execute name=mysql
CREATE TABLE drop_columns (f1 INTEGER, f2 INTEGER);
INSERT INTO drop_columns VALUES (1, 1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.drop_columns);

> SELECT * FROM drop_columns;
1 1

$ mysql-execute name=mysql
ALTER TABLE drop_columns DROP COLUMN f2;

! SELECT * FROM drop_columns;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> MODIFY <col> TYPE
#

$ mysql-execute name=mysql
CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO alter_column VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column);

$ mysql-execute name=mysql
ALTER TABLE alter_column MODIFY f2 INT;
INSERT INTO alter_column VALUES (3, 23);

! SELECT * from alter_column;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE reorder column
#

$ mysql-execute name=mysql
CREATE TABLE reorder_column (f1 INTEGER, f2 INTEGER);
INSERT INTO reorder_column VALUES (1, 2);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.reorder_column);

$ mysql-execute name=mysql
ALTER TABLE reorder_column MODIFY f1 INT AFTER f2;
INSERT INTO reorder_column VALUES (20, 10);

! SELECT * from reorder_column;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;


#
# ALTER TABLE rename column
#

$ mysql-execute name=mysql
CREATE TABLE rename_column (f1 INTEGER, f2 INTEGER);
INSERT INTO rename_column VALUES (1, 2);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.rename_column);

$ mysql-execute name=mysql
ALTER TABLE rename_column CHANGE f1 f3 INT;
INSERT INTO rename_column VALUES (10, 20);

! SELECT * from rename_column;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;


#
# ALTER TABLE no column change
#

$ mysql-execute name=mysql
CREATE TABLE no_column_change (f1 INT);
INSERT INTO no_column_change VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.no_column_change);

$ mysql-execute name=mysql
ALTER TABLE no_column_change CHANGE f1 f1 INT;
INSERT INTO no_column_change VALUES (10);

> SELECT * from no_column_change;
1
10

> DROP SOURCE mz_source CASCADE;

#
# Alter a column type using a fully-qualified name
# to validate our detection of ALTER TABLE <schema>.<table>
#

$ mysql-execute name=mysql
CREATE TABLE alter_column_2 (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO alter_column_2 VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column_2);

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS other;
CREATE DATABASE other;
USE other;
ALTER TABLE public.alter_column_2 MODIFY f2 INT;
INSERT INTO public.alter_column_2 VALUES (3, 23);

! SELECT * from alter_column_2;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> MODIFY <col> TYPE NON NULLABLE
# Adding a non-null constraint should be allowed
#

$ mysql-execute name=mysql
USE public;
CREATE TABLE alter_column_nullable (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO alter_column_nullable VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column_nullable);

$ mysql-execute name=mysql
ALTER TABLE alter_column_nullable MODIFY f2 VARCHAR(2) NOT NULL;
INSERT INTO alter_column_nullable VALUES (3, '23');

> SELECT * from alter_column_nullable;
2 12
3 23

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> MODIFY <col> TYPE NULLABLE
# Removing a non-null constraint should error
#

$ mysql-execute name=mysql
CREATE TABLE alter_column_nullable_2 (f1 INTEGER, f2 VARCHAR(2) NOT NULL);
INSERT INTO alter_column_nullable_2 VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column_nullable_2);

$ mysql-execute name=mysql
ALTER TABLE alter_column_nullable_2 MODIFY f2 VARCHAR(2);
INSERT INTO alter_column_nullable_2 VALUES (3, '23');

! SELECT * from alter_column_nullable_2;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> ADD CONSTRAINT <uq> UNIQUE (col);
# Adding a new unique constraint should be allowed
#

$ mysql-execute name=mysql
CREATE TABLE alter_column_uq (f1 INTEGER, f2 VARCHAR(2));
INSERT INTO alter_column_uq VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column_uq);

$ mysql-execute name=mysql
ALTER TABLE alter_column_uq ADD CONSTRAINT uq_f2 UNIQUE (f1);
INSERT INTO alter_column_uq VALUES (3, '23');

> SELECT * from alter_column_uq;
2 12
3 23

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE <tbl> DROP INDEX <uq>;
# Dropping new unique constraint should error
#

$ mysql-execute name=mysql
CREATE TABLE alter_column_uq_2 (f1 INTEGER, f2 VARCHAR(2));
ALTER TABLE alter_column_uq_2 ADD CONSTRAINT uq_f2 UNIQUE (f1);
INSERT INTO alter_column_uq_2 VALUES (2, '12');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_column_uq_2);

$ mysql-execute name=mysql
ALTER TABLE alter_column_uq_2 DROP INDEX uq_f2;
INSERT INTO alter_column_uq_2 VALUES (3, '23');

! SELECT * from alter_column_uq_2;
contains:incompatible schema change

> DROP SOURCE mz_source CASCADE;

#
# ALTER TABLE old_table RENAME new_table
#

$ mysql-execute name=mysql
USE public;
CREATE TABLE alter_table_rename (f1 INTEGER);
INSERT INTO alter_table_rename VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_table_rename);

$ mysql-execute name=mysql
ALTER TABLE alter_table_rename RENAME TO alter_table_renamed;

! SELECT * FROM alter_table_rename;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

#
# RENAME TABLE old_table TO new_table
#

$ mysql-execute name=mysql
CREATE TABLE alter_table_rename_2 (f1 INTEGER);
INSERT INTO alter_table_rename_2 VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.alter_table_rename_2);

$ mysql-execute name=mysql
RENAME TABLE alter_table_rename_2 TO alter_table_rename_3;

! SELECT * FROM alter_table_rename_2;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

#
# DROP TABLE <tbl>
#

$ mysql-execute name=mysql
CREATE TABLE drop_table (f1 INTEGER);
INSERT INTO drop_table VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.drop_table);

$ mysql-execute name=mysql
DROP TABLE drop_table;

! SELECT * FROM drop_table;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

#
# TRUNCATE TABLE <tbl>
#

$ mysql-execute name=mysql
CREATE TABLE trunc_table (f1 INTEGER);
INSERT INTO trunc_table VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.trunc_table);

$ mysql-execute name=mysql
TRUNCATE TABLE trunc_table;

! SELECT * FROM trunc_table;
contains:table was truncated

> DROP SOURCE mz_source CASCADE;

#
# TRUNCATE <tbl>
#

$ mysql-execute name=mysql
CREATE TABLE trunc_table_2 (f1 INTEGER);
INSERT INTO trunc_table_2 VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.trunc_table_2);

$ mysql-execute name=mysql
TRUNCATE trunc_table_2;

! SELECT * FROM trunc_table_2;
contains:table was truncated

> DROP SOURCE mz_source CASCADE;
