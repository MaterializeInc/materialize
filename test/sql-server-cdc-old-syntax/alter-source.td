# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_a VALUES (1, 'one');
INSERT INTO table_a VALUES (2, 'two');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_a', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;
> DROP SOURCE dummy;

> SELECT * FROM table_a;
1 one
2 two

$ sql-server-execute name=sql-server
DROP TABLE table_a;

! ALTER SOURCE mz_source ADD SUBSOURCE table_b;
contains:reference to table_b not found in source

# Adding a table with the same name as a running table does not allow you to add
# the new table, even though its OID is the different.

$ sql-server-execute name=sql-server
CREATE TABLE table_a (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'table_a', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_a', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'table_a2';
INSERT INTO table_a VALUES (9, 'nine');

# Current table_a is not new table_a. Note that this only works right now
# because we are bad at detecting dropped tables.
> SELECT * FROM table_a;
1 one
2 two

# We are not aware that the new table_a is different
!ALTER SOURCE mz_source ADD SUBSOURCE table_a;
contains:catalog item 'table_a' already exists

> DROP SOURCE mz_source CASCADE;

# Re-populate tables for rest of test.

$ sql-server-execute name=sql-server
DELETE FROM table_a;
INSERT INTO table_a VALUES (1, 'one');
INSERT INTO table_a VALUES (2, 'two');

CREATE TABLE table_b (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_b VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_b', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_b VALUES (2, 'two');

CREATE TABLE table_c (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_c VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_c', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_c VALUES (2, 'two');

CREATE TABLE table_d (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_d VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_d', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_d VALUES (2, 'two');

CREATE TABLE table_e (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_e VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_e', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_e VALUES (2, 'two');

CREATE TABLE table_f (pk INTEGER PRIMARY KEY, f2 VARCHAR(10));
INSERT INTO table_f VALUES (1, 'var0');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_f', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_f VALUES (2, 'var1');

CREATE TABLE table_g (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_g VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_g', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_g VALUES (2, 'two');

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection (
    TEXT COLUMNS (dbo.table_f.f2)
  )
  FOR ALL TABLES;

> DROP SOURCE dummy;

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_a               subsource
table_b               subsource
table_c               subsource
table_d               subsource
table_e               subsource
table_f               subsource
table_g               subsource

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
dbo.table_f.f2

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_f);
"f2"

#
# State checking
#
> DROP SOURCE table_a

> SELECT * FROM table_b;
1 one
2 two

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_b               subsource
table_c               subsource
table_d               subsource
table_e               subsource
table_f               subsource
table_g               subsource

! SELECT * FROM table_a;
contains: unknown catalog item 'table_a'

# Makes progress after dropping subsources
$ sql-server-execute name=sql-server
INSERT INTO table_b VALUES (3, 'three');

> SELECT * FROM table_b;
1 one
2 two
3 three

# IF EXISTS works
> DROP SOURCE IF EXISTS table_a;

# Multiple, repetitive tables work
> DROP SOURCE table_b, table_c, table_b, table_c, table_b, table_c;

# IF EXISTS works with multiple tables
> DROP SOURCE IF EXISTS table_c, table_d;

> CREATE MATERIALIZED VIEW mv_e AS SELECT pk + 1 FROM table_e;
> CREATE MATERIALIZED VIEW mv_f AS SELECT pk + 1 FROM table_f;

# Makes progress after dropping subsources
$ sql-server-execute name=sql-server
INSERT INTO table_e VALUES (3, 'three');

> SELECT * FROM mv_e;
2
3
4

> SELECT name FROM mz_materialized_views
mv_e
mv_f

# RESTRICT works
! DROP SOURCE table_e RESTRICT;
contains:cannot drop source "table_e": still depended upon by materialized view "mv_e"

# CASCADE works
> DROP SOURCE table_e CASCADE;

# IF NOT EXISTS + CASCADE works
> DROP SOURCE IF EXISTS table_e, table_f CASCADE;

# TEXT COLUMNS removed from table_f
> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress
table_g               subsource

> SELECT name FROM mz_materialized_views

> DROP SOURCE table_g;

> SHOW SUBSOURCES ON mz_source
mz_source_progress    progress

#
# Add subsources

> ALTER SOURCE mz_source ADD SUBSOURCE table_g;

! ALTER SOURCE mz_source ADD SUBSOURCE table_g;
contains:catalog item 'table_g' already exists

> ALTER SOURCE mz_source ADD SUBSOURCE table_a, table_b AS tb;

> SELECT * FROM table_a;
1 one
2 two

! ALTER SOURCE mz_source ADD SUBSOURCE table_a;
contains:catalog item 'table_a' already exists

> SELECT * FROM tb;
1 one
2 two
3 three

!SELECT * FROM table_b;
contains:unknown catalog item

# We can add tables that didn't exist at the time of publication
$ sql-server-execute name=sql-server
CREATE TABLE table_h (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_h VALUES (1, 'one');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_h', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_h VALUES (2, 'two');

> ALTER SOURCE mz_source ADD SUBSOURCE table_h;

> SELECT * FROM table_h;
1 one
2 two

> SHOW SUBSOURCES ON mz_source
mz_source_progress progress
table_a            subsource
table_g            subsource
table_h            subsource
tb                 subsource

#
# Complex subsource operations

# If your schema change breaks the subsource, you can fix it.
$ sql-server-execute name=sql-server
ALTER TABLE table_a DROP COLUMN f2;

! SELECT * FROM table_a;
contains:Incompatible schema change

> SELECT error ~~ '%Incompatible schema change%' FROM mz_internal.mz_source_statuses WHERE name = 'table_a';
true

# Subsource errors not propagated to primary source
> SELECT error IS NULL FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

> DROP SOURCE table_a;
> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1
2

# If you add columns you can re-ingest them after recreating the capture instance
$ sql-server-execute name=sql-server
ALTER TABLE table_a ADD f2 VARCHAR(128);
INSERT INTO table_a VALUES (3, 'three');

> SELECT * FROM table_a;
1
2
3

> DROP SOURCE table_a;

$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'table_a', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_a', @role_name = 'SA', @supports_net_changes = 0;

> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1 <null>
2 <null>
3 three

# If you add a NOT NULL constraint, you can propagate it.  And we added a column, so recreate the
# capture instance.
$ sql-server-execute name=sql-server
ALTER TABLE table_a ADD f3 int DEFAULT 1 NOT NULL;
INSERT INTO table_a VALUES (4, 'four', 4);

> DROP SOURCE table_a;

$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'table_a', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_a', @role_name = 'SA', @supports_net_changes = 0;

> ALTER SOURCE mz_source ADD SUBSOURCE table_a;

> SELECT * FROM table_a;
1 <null> 1
2 <null> 1
3 three 1
4 four 4

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM table_a WHERE f3 IS NULL;
Explained Query (fast path):
  Constant <empty>

Target cluster: quickstart

# Can add tables with text columns
! ALTER SOURCE mz_source ADD SUBSOURCE table_f WITH (TEXT COLUMNS (dbo.table_f.f2, dbo.table_f.f2));
contains: invalid TEXT COLUMNS option value: unexpected multiple references to dbo.table_f.f2

> ALTER SOURCE mz_source ADD SUBSOURCE table_f WITH (TEXT COLUMNS (dbo.table_f.f2));

> SELECT * FROM table_f
1 var0
2 var1

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
dbo.table_f.f2

# Drop a table that's in the publication, which shuffles the tables' output
# indexes, then add a table to the publication and ensure it can be added.
$ sql-server-execute name=sql-server
DROP TABLE table_c, table_d;

CREATE TABLE table_i (pk INTEGER PRIMARY KEY, f2 VARCHAR(128));
INSERT INTO table_i VALUES (1, 'var0');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_i', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_i VALUES (2, 'var1');

INSERT INTO table_f VALUES (3, 'var1');

> ALTER SOURCE mz_source ADD SUBSOURCE table_i WITH (TEXT COLUMNS (dbo.table_i.f2));

# TODO: This should work
# > SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
# "dbo.table_f.f2, dbo.table_i.f2"
#
# > SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE table_i);
# f2

> SELECT * FROM table_f
1 var0
2 var1
3 var1

> DROP SOURCE table_f, table_i;

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source);
<null>

! ALTER SOURCE mz_source ADD SUBSOURCE table_e WITH (TEXT COLUMNS (dbo.table_z.a));
contains:SQL SERVER source validation: TEXT COLUMNS refers to table not currently being added

! ALTER SOURCE mz_source ADD SUBSOURCE table_e WITH (TEXT COLUMNS (dbo.table_f.f2));
contains:SQL SERVER source validation: TEXT COLUMNS refers to table not currently being added
detail:the following columns are referenced but not added: dbo.table_f.f2

# Test adding text cols w/o original text columns

> CREATE SOURCE "mz_source_wo_init_text_cols"
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR TABLES (table_a AS t_a);

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
<null>

> ALTER SOURCE mz_source_wo_init_text_cols ADD SUBSOURCE table_f AS t_f WITH (TEXT COLUMNS (dbo.table_f.f2));

> SELECT regexp_match(create_sql, 'TEXT COLUMNS = \((.*?)\)')[1] FROM (SHOW CREATE SOURCE mz_source_wo_init_text_cols);
dbo.table_f.f2

# add a table after having created the source
$ sql-server-execute name=sql-server
CREATE TABLE t2 (f1 BIT);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;

! SELECT COUNT(*) > 0 FROM t2;
contains:unknown catalog item 't2'
