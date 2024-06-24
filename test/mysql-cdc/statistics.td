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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


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

> CREATE CLUSTER stats_cluster SIZE '${arg.default-replica-size}'

> CREATE SOURCE mz_source IN CLUSTER stats_cluster FROM MYSQL CONNECTION mysqc FOR TABLES (public.t1);

> SELECT COUNT(*) > 0 FROM t1;
true

> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1

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
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1

$ set-from-sql var=pre-restart-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 0)

$ mysql-execute name=mysql
INSERT INTO t1 VALUES ('three');

> ALTER CLUSTER stats_cluster SET (REPLICATION FACTOR 1)

# Ensure the snapshot stats stay there, and don't change.
> SELECT
    s.name,
    SUM(u.offset_committed) > ${pre-restart-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed),
    SUM(u.snapshot_records_known),
    SUM(u.snapshot_records_staged)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true 1 1

# TODO(guswynn/roshan): test snapshot stats when alter cluster add table is supported by mysql

> DROP CLUSTER stats_cluster CASCADE
