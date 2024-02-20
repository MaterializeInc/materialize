# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test progress statistics
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 TEXT);

INSERT INTO t1 VALUES ('one');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysqc FOR TABLES (public.t1);

> SELECT COUNT(*) > 0 FROM t1;
true

# statistics are only populated every minute by default
$ set-sql-timeout duration=2minutes

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true

$ set-from-sql var=previous-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')


$ mysql-execute name=mysql
INSERT INTO t1 VALUES ('two');


> SELECT
    s.name,
    SUM(u.offset_committed) > ${previous-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true
