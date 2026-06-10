# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# MySQL booted with ANSI_QUOTES in its sql_mode (via `--sql-mode` in the
# cdc-ansi-quotes workflow), so every connection -- Materialize's and the
# testdrive client's -- is under it from the start and `"` quotes identifiers.
# Companion to ../ansi-quotes.td, which covers the runtime SET GLOBAL variant.

$ set-sql-timeout duration=120s

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# This client connected to the ANSI_QUOTES server, so it is itself under
# ANSI_QUOTES and quotes the space-containing name with double quotes -- the
# genuine ANSI_QUOTES identifier syntax.
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE ansi_server (f1 INTEGER);
CREATE TABLE "ansi spaced" (f1 INTEGER);
# An ANSI-quoted name whose body is literal backticks: the double quotes are the
# identifier delimiters, the backticks are part of the name. This is the hardest
# case for binlog normalization to round-trip back to backtick quoting.
CREATE TABLE "`ansi_backticks`" (f1 INTEGER);
INSERT INTO ansi_server VALUES (1);
INSERT INTO "ansi spaced" VALUES (1);
INSERT INTO "`ansi_backticks`" VALUES (1);

# Source creation + snapshot run against the ANSI_QUOTES server.
> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE ansi_server FROM SOURCE mz_source (REFERENCE public.ansi_server);
> CREATE TABLE ansi_spaced FROM SOURCE mz_source (REFERENCE public."ansi spaced");
> CREATE TABLE ansi_backticks FROM SOURCE mz_source (REFERENCE public."`ansi_backticks`");
> COMMIT

> SELECT * FROM ansi_server;
1
> SELECT * FROM ansi_spaced;
1
> SELECT * FROM ansi_backticks;
1

# Replication-time DDL detection must also work against this server.
$ mysql-execute name=mysql
INSERT INTO ansi_server VALUES (2);
INSERT INTO "ansi spaced" VALUES (2);
INSERT INTO "`ansi_backticks`" VALUES (2);

> SELECT * FROM ansi_server;
1
2
> SELECT * FROM ansi_spaced;
1
2
> SELECT * FROM ansi_backticks;
1
2

$ mysql-execute name=mysql
DROP TABLE ansi_server;
DROP TABLE "ansi spaced";
DROP TABLE "`ansi_backticks`";

! SELECT * FROM ansi_server;
contains:table was dropped
! SELECT * FROM ansi_spaced;
contains:table was dropped
! SELECT * FROM ansi_backticks;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;
