# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# MySQL with ANSI_QUOTES enabled at runtime via SET GLOBAL before Materialize
# connects, so its snapshot and replication connections inherit it and `"` quotes
# identifiers. The full source lifecycle must work. Companion to
# override/20-ansi-quotes.td, which covers the boot-time variant.

$ set-sql-timeout duration=120s

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Enable ANSI_QUOTES globally before Materialize connects -- SET GLOBAL only
# affects connections opened afterwards. This session opened earlier, so it stays
# in the default sql_mode and quotes `ansi spaced` with backticks; Materialize
# connects later (at CREATE SOURCE) and sees ANSI_QUOTES.
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

# Restore the global sql_mode so sibling testdrive files sharing this MySQL
# container are unaffected.
$ mysql-execute name=mysql
SET GLOBAL sql_mode = REPLACE(@@global.sql_mode, ',ANSI_QUOTES', '');
