# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

##############################################################################
# MySQL server running with `ANSI_QUOTES` in its *global* sql_mode.
#
# Unlike the session-scoped case in drop-table-detection.td, here ANSI_QUOTES is
# enabled globally *before* Materialize opens any connection. Materialize does
# not pin its own sql_mode on connect, so every connection it makes -- for
# snapshotting and for replication -- inherits ANSI_QUOTES, where `"` quotes
# identifiers instead of delimiting strings.
#
# This guards that Materialize's own generated SQL does not depend on `"..."`
# being a string literal: its information_schema introspection uses single
# quotes for strings and backticks for identifiers, both of which are valid
# under ANSI_QUOTES. The whole source lifecycle -- create, snapshot, replicate,
# detect DROP -- must work against such a server.
##############################################################################

$ set-sql-timeout duration=120s

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Enable ANSI_QUOTES globally before Materialize connects. SET GLOBAL only
# affects connections opened afterwards, so Materialize's connections (made at
# CREATE SOURCE time, below) pick it up.
# This mysql-execute connection was opened before the SET GLOBAL above, so its
# own session is still in the default sql_mode -- it quotes the space-containing
# name `ansi spaced` with backticks. Materialize, which connects later, sees
# ANSI_QUOTES and references the same table with double quotes.
$ mysql-execute name=mysql
SET GLOBAL sql_mode = CONCAT(@@global.sql_mode, ',ANSI_QUOTES');
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE ansi_global (f1 INTEGER);
CREATE TABLE `ansi spaced` (f1 INTEGER);
INSERT INTO ansi_global VALUES (1);
INSERT INTO `ansi spaced` VALUES (1);

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

# Source creation + snapshot run against the global-ANSI_QUOTES server.
> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE ansi_global FROM SOURCE mz_source (REFERENCE public.ansi_global);
> CREATE TABLE ansi_spaced FROM SOURCE mz_source (REFERENCE public."ansi spaced");
> COMMIT

> SELECT * FROM ansi_global;
1
> SELECT * FROM ansi_spaced;
1

# Replication-time DDL detection must also work against this server.
$ mysql-execute name=mysql
INSERT INTO ansi_global VALUES (2);
INSERT INTO `ansi spaced` VALUES (2);

> SELECT * FROM ansi_global;
1
2
> SELECT * FROM ansi_spaced;
1
2

$ mysql-execute name=mysql
DROP TABLE ansi_global;
DROP TABLE `ansi spaced`;

! SELECT * FROM ansi_global;
contains:table was dropped
! SELECT * FROM ansi_spaced;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;

# Restore the global sql_mode so sibling testdrive files (which share this MySQL
# container within the workflow) are unaffected.
$ mysql-execute name=mysql
SET GLOBAL sql_mode = REPLACE(@@global.sql_mode, ',ANSI_QUOTES', '');
