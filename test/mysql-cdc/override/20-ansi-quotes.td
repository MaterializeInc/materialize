# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

##############################################################################
# MySQL server booted with ANSI_QUOTES in its sql_mode (configured via
# `--sql-mode` in the `cdc-ansi-quotes` workflow, not toggled at runtime). Every
# connection -- including the ones Materialize opens for snapshotting and
# replication, and the testdrive client below -- is under ANSI_QUOTES from the
# start, so `"` quotes identifiers rather than delimiting strings.
#
# The companion test ../ansi-quotes.td covers the runtime `SET GLOBAL` variant;
# this one covers the boot-time server configuration. The full source lifecycle
# (create, snapshot, replicate, detect DROP) must work against such a server.
##############################################################################

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
CREATE DATABASE public;
USE public;
CREATE TABLE ansi_server (f1 INTEGER);
INSERT INTO ansi_server VALUES (1);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE ansi_server FROM SOURCE mz_source (REFERENCE public.ansi_server);
> COMMIT

> SELECT * FROM ansi_server;
1

$ mysql-execute name=mysql
INSERT INTO ansi_server VALUES (2);

> SELECT * FROM ansi_server;
1
2

$ mysql-execute name=mysql
DROP TABLE ansi_server;

! SELECT * FROM ansi_server;
contains:table was dropped

> DROP SOURCE mz_source CASCADE;
