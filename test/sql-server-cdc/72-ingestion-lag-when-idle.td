# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test: a freshly created source must report a near-zero ingestion
# lag, even while its upstream database is idle.
#
# The two offset statistics must be initialized from a common validated
# upstream point. Leaving `offset_committed` at 0 after `offset_known` advances
# produces an enormous lag. Publishing the full resumption LSN before the known
# frontier catches up reverses the ordering and makes the same lag calculation
# (`offset_known - offset_committed`) underflow.
#
# A newly enabled capture instance can start ahead of the database-wide CDC
# maximum. Purification must use that higher start LSN as the source's resumption
# point. The progress probe still observes the lower database maximum. We stop
# this database's capture job before enabling the target table so the two LSNs
# cannot converge and mask the invalid statistics.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server split-lines=false
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'lag_test')
BEGIN
    ALTER DATABASE lag_test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE lag_test;
END;

$ sql-server-execute name=sql-server
CREATE DATABASE lag_test COLLATE Latin1_General_100_CI_AI_SC_UTF8;
USE lag_test;
EXEC sys.sp_cdc_enable_db;
ALTER DATABASE lag_test SET ALLOW_SNAPSHOT_ISOLATION ON;

$ sql-server-execute name=sql-server
USE lag_test;
CREATE TABLE lag_seed (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'lag_seed', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO lag_seed VALUES ('seed');

# Establish a nonzero database maximum, then freeze it. Enabling `lag_t` after
# this point gives its capture instance a later start LSN.
$ sql-server-execute name=sql-server split-lines=false
WAITFOR DELAY '00:00:20';

$ sql-server-execute name=sql-server
USE lag_test;
EXEC sys.sp_cdc_stop_job @job_type = 'capture';
CREATE TABLE lag_t (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'lag_t', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO lag_t VALUES ('one'), ('two'), ('three');

# This is the upstream state the test depends on. If SQL Server changes its CDC
# behavior, fail here rather than attributing a later result to Materialize.
$ sql-server-execute name=sql-server split-lines=false
IF NOT EXISTS (
    SELECT 1
    FROM lag_test.cdc.change_tables
    WHERE capture_instance = 'dbo_lag_t'
      AND start_lsn > lag_test.sys.fn_cdc_get_max_lsn()
)
    THROW 50000, 'expected capture start LSN above database maximum', 1;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000;
ALTER SYSTEM SET storage_statistics_interval = 2000;

> CREATE SECRET IF NOT EXISTS mspass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION msconn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE lag_test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET mspass
  );

> CREATE CLUSTER lag_cluster SIZE '${arg.default-replica-size}'
> CREATE SOURCE lag_source IN CLUSTER lag_cluster FROM SQL SERVER CONNECTION msconn;
> CREATE TABLE lag_t FROM SOURCE lag_source (REFERENCE dbo.lag_t);

# We intentionally do not wait for the snapshot to become queryable. On an idle
# database the source frontier only advances once SQL Server advances its max
# LSN, which can take minutes. That frontier advance is also what would fix a
# buggy `offset_committed`, so waiting for it would mask the bug. Instead we
# assert on the statistics, which are populated within seconds regardless of the
# frontier.

# Ensure statistics for the source's replica exist before pinning the replica
# id below, which does not retry on a missing row.
> SELECT COUNT(*) > 0
  FROM
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    cr.id = u.replica_id AND s.name = 'lag_source' AND u.id = s.id
true

$ set-from-sql var=replica_id
SELECT cr.id
  FROM
    mz_clusters c,
    mz_cluster_replicas cr,
    mz_internal.mz_source_statistics u,
    mz_sources s
  WHERE
    c.name = 'lag_cluster' AND c.id = cr.cluster_id AND cr.id = u.replica_id
    AND s.name = 'lag_source' AND u.id = s.id
  ORDER BY cr.id
  LIMIT 1

# The reported committed offset must not exceed the first known upstream offset,
# even though the actual resumption LSN does. The second expression is the
# ingestion-lag calculation used by customers. Since both columns are `uint8`,
# invalid ordering makes the subtraction itself fail instead of reporting lag.
$ set-sql-timeout duration=60s

> SELECT
    u.offset_committed > 0,
    u.offset_known >= u.offset_committed,
    u.offset_known - u.offset_committed < 1000000
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'lag_source' AND u.replica_id = '${replica_id}'
true true true
