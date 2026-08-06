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
# The `offset_committed` statistic must be seeded from the resumption LSN when
# the source starts. If it is left at the default 0 while `offset_known` tracks
# the (large) upstream LSN, the console computes an enormous, bogus ingestion
# lag (`offset_known - offset_committed`).
#
# An idle upstream database is what makes this reliably observable. Any activity
# would advance the source frontier and set `offset_committed` to a correct
# value, masking the bug. SQL Server tracks the max LSN per database, so we run
# this against a dedicated database with no activity of its own. The background
# ticker keeps writing to the shared `test` database, but that does not advance
# the max LSN observed here.

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
CREATE TABLE lag_t (f1 VARCHAR(20));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'lag_t', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO lag_t VALUES ('one'), ('two'), ('three');

# Let the capture instance drain the writes above and go idle, so this
# database's max LSN is frozen before we snapshot. Once idle, SQL Server does
# not advance the max LSN for several minutes, which keeps the source frontier
# from advancing on its own and thus exercises the startup seeding of
# `offset_committed`.
$ sql-server-execute name=sql-server split-lines=false
WAITFOR DELAY '00:00:20';

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

# With `offset_committed` seeded from the resumption LSN it tracks `offset_known`,
# so the lag stays near zero even though the upstream database is idle. Without
# the fix `offset_committed` stays at 0 and the lag is the full upstream LSN
# until the idle database eventually advances its max LSN, which takes minutes.
# The bounded timeout is shorter than that heal time, so a buggy build fails
# here, while a correct build passes within seconds. The bound is phrased as an
# addition rather than `offset_known - offset_committed` so it never underflows
# the unsigned columns if `offset_known` is momentarily unset before the first
# probe tick.
$ set-sql-timeout duration=60s

> SELECT
    u.offset_committed > 0,
    u.offset_known >= u.offset_committed,
    u.offset_known < u.offset_committed + 1000000
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'lag_source' AND u.replica_id = '${replica_id}'
true true true
