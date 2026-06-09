# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Testing successful round trip of (potentially messy/complex) `DROP TABLE` commands.
# Added for SS-104 where we start parsing table identifiers rather than diffing against mysql.

$ set-sql-timeout duration=120s

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
DROP DATABASE IF EXISTS other;
CREATE DATABASE public;
CREATE DATABASE other;
USE public;


# 1. Single table

$ mysql-execute name=mysql
CREATE TABLE simple_drop (f1 INTEGER);
INSERT INTO simple_drop VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE simple_drop FROM SOURCE mz_source (REFERENCE public.simple_drop);
> COMMIT

> SELECT * FROM simple_drop;
1

$ mysql-execute name=mysql
DROP TABLE simple_drop;

! SELECT * FROM simple_drop;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 2. Multiple tables dropped in a single statement.

$ mysql-execute name=mysql
CREATE TABLE multi_a (f1 INTEGER);
CREATE TABLE multi_b (f1 INTEGER);
CREATE TABLE `multi /* c */` (f1 INTEGER);
INSERT INTO multi_a VALUES (1);
INSERT INTO multi_b VALUES (2);
INSERT INTO `multi /* c */` VALUES (3);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE multi_a FROM SOURCE mz_source (REFERENCE public.multi_a);
> CREATE TABLE multi_b FROM SOURCE mz_source (REFERENCE public.multi_b);
> CREATE TABLE multi_c FROM SOURCE mz_source (REFERENCE public."multi /* c */");
> COMMIT

> SELECT * FROM multi_a;
1
> SELECT * FROM multi_b;
2
> SELECT * FROM multi_c;
3

$ mysql-execute name=mysql
DROP TABLE multi_a, multi_b, `multi /* c */`;

! SELECT * FROM multi_a;
contains:table was dropped
! SELECT * FROM multi_b;
contains:table was dropped
! SELECT * FROM multi_c;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 3. `DROP TABLE IF EXISTS <tbl>` is detected (the IF EXISTS clause is skipped).

$ mysql-execute name=mysql
CREATE TABLE if_exists_drop (f1 INTEGER);
INSERT INTO if_exists_drop VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE if_exists_drop FROM SOURCE mz_source (REFERENCE public.if_exists_drop);
> COMMIT

> SELECT * FROM if_exists_drop;
1

$ mysql-execute name=mysql
DROP TABLE IF EXISTS if_exists_drop;

! SELECT * FROM if_exists_drop;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 4. Qualified `schema.table` names.
#
# When a table is dropped while a *different* database is current, MySQL logs
# the fully-qualified name (DROP TABLE `public`.`qualified_drop`). This must be
# resolved to the right schema+table. A second drop crosses schemas in a single
# statement.

$ mysql-execute name=mysql
USE public;
CREATE TABLE qualified_drop (f1 INTEGER);
INSERT INTO qualified_drop VALUES (1);
USE other;
CREATE TABLE cross_schema (f1 INTEGER);
INSERT INTO cross_schema VALUES (2);
USE public;

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE qualified_drop FROM SOURCE mz_source (REFERENCE public.qualified_drop);
> CREATE TABLE cross_schema FROM SOURCE mz_source (REFERENCE other.cross_schema);
> COMMIT

> SELECT * FROM qualified_drop;
1
> SELECT * FROM cross_schema;
2

# Drop both from a current database that matches neither, forcing MySQL to
# log both names fully qualified, in one statement.
$ mysql-execute name=mysql
USE mysql;
DROP TABLE public.qualified_drop, other.cross_schema;
USE public;

! SELECT * FROM qualified_drop;
contains:table was dropped
! SELECT * FROM cross_schema;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 5. An unqualified name resolves against the *current* schema.
#
# A table `keep_me` exists in both `public` and `other`. We track only
# public.keep_me. Dropping the unqualified `keep_me` while `other` is current
# refers to other.keep_me and must NOT touch public.keep_me, which keeps
# ingesting.

$ mysql-execute name=mysql
USE public;
CREATE TABLE keep_me (f1 INTEGER);
INSERT INTO keep_me VALUES (1);
USE other;
CREATE TABLE keep_me (f1 INTEGER);
INSERT INTO keep_me VALUES (100);
USE public;

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE keep_me FROM SOURCE mz_source (REFERENCE public.keep_me);
> CREATE TABLE delete_me FROM SOURCE mz_source (REFERENCE other.keep_me);
> COMMIT

> SELECT * FROM keep_me;
1

> SELECT * FROM delete_me;
100

# Unqualified drop while `other` is current -> drops other.keep_me only.
$ mysql-execute name=mysql
USE other;
DROP TABLE keep_me;
USE public;
INSERT INTO keep_me VALUES (2);

! SELECT * FROM delete_me;
contains:table was dropped

# public.keep_me is unaffected and keeps replicating.
> SELECT * FROM keep_me;
1
2

> DROP SOURCE mz_source CASCADE;

# 6. One upstream table feeding several Materialize tables.
#
# Dropping the single upstream table must error every Materialize output that
# references it.

$ mysql-execute name=mysql
USE public;
CREATE TABLE fanout (f1 INTEGER);
INSERT INTO fanout VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE fanout_one FROM SOURCE mz_source (REFERENCE public.fanout);
> CREATE TABLE fanout_two FROM SOURCE mz_source (REFERENCE public.fanout);
> COMMIT

> SELECT * FROM fanout_one;
1
> SELECT * FROM fanout_two;
1

$ mysql-execute name=mysql
DROP TABLE fanout;

! SELECT * FROM fanout_one;
contains:table was dropped
! SELECT * FROM fanout_two;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;


# 7. The same upstream table tracked by two independent sources.
#
# Each source has its own replication stream and detects the drop on its own;
# both must error.

$ mysql-execute name=mysql
CREATE TABLE shared_tbl (f1 INTEGER);
INSERT INTO shared_tbl VALUES (1);

> BEGIN
> CREATE SOURCE mz_source_a FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE shared_a FROM SOURCE mz_source_a (REFERENCE public.shared_tbl);
> COMMIT

> BEGIN
> CREATE SOURCE mz_source_b FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE shared_b FROM SOURCE mz_source_b (REFERENCE public.shared_tbl);
> COMMIT

> SELECT * FROM shared_a;
1
> SELECT * FROM shared_b;
1

$ mysql-execute name=mysql
DROP TABLE shared_tbl;

! SELECT * FROM shared_a;
contains:table was dropped
! SELECT * FROM shared_b;
contains:table was dropped

> DROP SOURCE mz_source_a CASCADE;
> DROP SOURCE mz_source_b CASCADE;

# 8. Selective drop: only the named tables error; siblings keep ingesting.

$ mysql-execute name=mysql
CREATE TABLE sel_drop (f1 INTEGER);
CREATE TABLE sel_keep (f1 INTEGER);
INSERT INTO sel_drop VALUES (1);
INSERT INTO sel_keep VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE sel_drop FROM SOURCE mz_source (REFERENCE public.sel_drop);
> CREATE TABLE sel_keep FROM SOURCE mz_source (REFERENCE public.sel_keep);
> COMMIT

> SELECT * FROM sel_drop;
1
> SELECT * FROM sel_keep;
1

$ mysql-execute name=mysql
DROP TABLE sel_drop;
INSERT INTO sel_keep VALUES (2);

! SELECT * FROM sel_drop;
contains:table was dropped

# The sibling is unaffected and continues to replicate new rows.
> SELECT * FROM sel_keep;
1
2

> DROP SOURCE mz_source CASCADE;

# 9. Comments in awkward positions must not cause missed drop. This test is 
# more testing mysql's binlog replication scrubbing comments than excercising Materialize
# but is an important confirmation (especially because DROP /* comment */ TABLE) would fail
# in Materialize today if it got through.

$ mysql-execute name=mysql
CREATE TABLE c_block_before (f1 INTEGER);
CREATE TABLE c_block_trailing (f1 INTEGER);
CREATE TABLE c_dash_line (f1 INTEGER);
CREATE TABLE c_hash_line (f1 INTEGER);
CREATE TABLE c_executable (f1 INTEGER);
CREATE TABLE c_between (f1 INTEGER);
INSERT INTO c_block_before VALUES (1);
INSERT INTO c_block_trailing VALUES (1);
INSERT INTO c_dash_line VALUES (1);
INSERT INTO c_hash_line VALUES (1);
INSERT INTO c_executable VALUES (1);
INSERT INTO c_between VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE c_block_before FROM SOURCE mz_source (REFERENCE public.c_block_before);
> CREATE TABLE c_block_trailing FROM SOURCE mz_source (REFERENCE public.c_block_trailing);
> CREATE TABLE c_dash_line FROM SOURCE mz_source (REFERENCE public.c_dash_line);
> CREATE TABLE c_hash_line FROM SOURCE mz_source (REFERENCE public.c_hash_line);
> CREATE TABLE c_executable FROM SOURCE mz_source (REFERENCE public.c_executable);
> CREATE TABLE c_between FROM SOURCE mz_source (REFERENCE public.c_between);
> COMMIT

> SELECT count(*) FROM c_block_before;
1
> SELECT count(*) FROM c_between;
1

# Block comment before the table name.
$ mysql-execute name=mysql
DROP TABLE /* drop this one */ c_block_before;

# Trailing block comment after the table name.
$ mysql-execute name=mysql
DROP TABLE c_block_trailing /* trailing comment */;

# `--` line comment (the trailing newline terminates the statement).
$ mysql-execute name=mysql
DROP TABLE c_dash_line -- end of line comment

# `#` MySQL line comment.
$ mysql-execute name=mysql
DROP TABLE c_hash_line # mysql line comment

# Executable comment: its body is real SQL, so the table inside is dropped.
$ mysql-execute name=mysql
DROP TABLE /*!40000 c_executable */;

# Comment between DROP and TABLE -- the dispatch-gap guard.
$ mysql-execute name=mysql
DROP /* comment between keywords */ TABLE c_between;

! SELECT * FROM c_block_before;
contains:table was dropped
! SELECT * FROM c_block_trailing;
contains:table was dropped
! SELECT * FROM c_dash_line;
contains:table was dropped
! SELECT * FROM c_hash_line;
contains:table was dropped
! SELECT * FROM c_executable;
contains:table was dropped
! SELECT * FROM c_between;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 10. Escape-shaped characters in identifiers must survive the round trip.
#
# These names contain characters that are only meaningful at the SQL level --
# a doubled backtick (an escaped literal backtick), a dot, comment delimiters,
# a `--`, a `#`, and a `;`. None of them is a separator or a comment here: each
# is part of a single quoted identifier. The detector reads sqlparser's decoded
# identifier rather than re-splitting raw SQL, so the table name must match
# exactly and the drop must be detected.
#
# A column name carrying a double-quote (`c"d`) is included to make sure odd
# column names do not interfere with snapshotting or detection.

$ mysql-execute name=mysql
CREATE TABLE `a``b` (`c"d` INTEGER);
CREATE TABLE `weird.name` (f1 INTEGER);
CREATE TABLE `tbl /* not a comment */` (f1 INTEGER);
CREATE TABLE `dash--in--name` (f1 INTEGER);
CREATE TABLE `hash#in#name` (f1 INTEGER);
CREATE TABLE `semi;colon` (f1 INTEGER);
INSERT INTO `a``b` VALUES (42);
INSERT INTO `weird.name` VALUES (7);
INSERT INTO `tbl /* not a comment */` VALUES (1);
INSERT INTO `dash--in--name` VALUES (1);
INSERT INTO `hash#in#name` VALUES (1);
INSERT INTO `semi;colon` VALUES (1);

# On the Materialize side these upstream names are referenced with standard
# double-quoted identifiers; the inner backtick / dot / comment text / `;` are
# all literal.
> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE escaped_backtick FROM SOURCE mz_source (REFERENCE public."a`b");
> CREATE TABLE dotted_name FROM SOURCE mz_source (REFERENCE public."weird.name");
> CREATE TABLE comment_shaped FROM SOURCE mz_source (REFERENCE public."tbl /* not a comment */");
> CREATE TABLE dash_name FROM SOURCE mz_source (REFERENCE public."dash--in--name");
> CREATE TABLE hash_name FROM SOURCE mz_source (REFERENCE public."hash#in#name");
> CREATE TABLE semi_name FROM SOURCE mz_source (REFERENCE public."semi;colon");
> COMMIT

# The escaped-backtick table really is named a`b and snapshots its data,
# including the oddly named column.
> SELECT * FROM escaped_backtick;
42
> SELECT * FROM dotted_name;
7
> SELECT * FROM comment_shaped;
1
> SELECT * FROM dash_name;
1
> SELECT * FROM hash_name;
1
> SELECT * FROM semi_name;
1

$ mysql-execute name=mysql
DROP TABLE `a``b`;
DROP TABLE `weird.name`;
DROP TABLE `tbl /* not a comment */`;
DROP TABLE `dash--in--name`;
DROP TABLE `hash#in#name`;
DROP TABLE `semi;colon`;

! SELECT * FROM escaped_backtick;
contains:table was dropped
! SELECT * FROM dotted_name;
contains:table was dropped
! SELECT * FROM comment_shaped;
contains:table was dropped
! SELECT * FROM dash_name;
contains:table was dropped
! SELECT * FROM hash_name;
contains:table was dropped
! SELECT * FROM semi_name;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# 11. The error is permanent: it cannot be undone from upstream.
#
# A dropped table's error is a definite, persisted error. Re-creating the
# table with the same name upstream does NOT clear it, and neither does
# restarting the replica. This is the corruption scenario the feature makes
# explicit rather than silently mis-handle.

> CREATE CLUSTER sticky_cluster SIZE '${arg.default-replica-size}';

$ mysql-execute name=mysql
CREATE TABLE sticky (f1 INTEGER);
CREATE TABLE canary (f1 INTEGER);
INSERT INTO sticky VALUES (1), (2);
INSERT INTO canary VALUES (100);

> BEGIN
> CREATE SOURCE sticky_source IN CLUSTER sticky_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE sticky FROM SOURCE sticky_source (REFERENCE public.sticky);
> CREATE TABLE canary FROM SOURCE sticky_source (REFERENCE public.canary);
> COMMIT

> SELECT * FROM sticky;
1
2
> SELECT * FROM canary;
100

# Drop the upstream table: the output goes into a definite error.
$ mysql-execute name=mysql
DROP TABLE sticky;

! SELECT * FROM sticky;
contains:table was dropped

# Re-create the same table name upstream and write to it. The canary write is
# ordered strictly after the re-create, so seeing it confirms Materialize has
# processed past the re-create.
$ mysql-execute name=mysql
CREATE TABLE sticky (f1 INTEGER);
INSERT INTO sticky VALUES (3);
INSERT INTO canary VALUES (200);

> SELECT * FROM canary;
100
200

# Re-creating the table upstream did NOT clear the error.
! SELECT * FROM sticky;
contains:table was dropped

# Restart the replica by cycling the replication factor, forcing the source
# dataflow to rehydrate from persisted state and resume replication.
> ALTER CLUSTER sticky_cluster SET (REPLICATION FACTOR 0)

> SELECT count(*)
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON (c.id = cr.cluster_id)
  WHERE c.name = 'sticky_cluster';
0

> ALTER CLUSTER sticky_cluster SET (REPLICATION FACTOR 1)

# Prove the source resumed after the restart: a fresh canary row appears.
$ mysql-execute name=mysql
INSERT INTO canary VALUES (300);

> SELECT * FROM canary;
100
200
300

# The error survives a replica restart.
! SELECT * FROM sticky;
contains:table was dropped

> DROP SOURCE sticky_source CASCADE;
> DROP CLUSTER sticky_cluster CASCADE;
