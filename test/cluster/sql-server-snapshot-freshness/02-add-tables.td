# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The `sql_server_snapshot_pause=return(t_paused)` failpoint is active on the
# storage cluster. Once the tables below are added, `t_paused`'s snapshot read
# hangs. This phase asserts what holds while a snapshot runs.
#
# - The hydrated table keeps ingesting CDC.
# - A table added together with the paused one completes its own snapshot
#   without waiting.
#
# This relies on `storage_persist_sink_description_lookahead`, which is
# enabled by default in the test configuration.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

# Adding both tables in one transaction restarts the ingestion dataflow once.
# Both tables then snapshot in the same incarnation.
> BEGIN
> CREATE TABLE t_alongside FROM SOURCE mz_source (REFERENCE dbo.t_alongside);
> CREATE TABLE t_paused FROM SOURCE mz_source (REFERENCE dbo.t_paused);
> COMMIT

# A table added alongside the paused one completes its snapshot.
> SELECT * FROM t_alongside
10
20

# The hydrated table continues to ingest CDC while the snapshot is paused.
$ sql-server-execute name=sql-server
INSERT INTO t_before VALUES (2);

> SELECT * FROM t_before
1
2

# And the alongside table ingests CDC once its own snapshot is done.
$ sql-server-execute name=sql-server
INSERT INTO t_alongside VALUES (30);

> SELECT * FROM t_alongside
10
20
30

# CDC for the paused table streams concurrently with its held snapshot. The
# persist sink stages the updates. They cannot commit until the snapshot
# completes and releases the table's frontier.
$ sql-server-execute name=sql-server
INSERT INTO t_paused VALUES (300);

# The paused table has staged updates but its snapshot has not committed.
# snapshot_committed stays false while the failpoint holds the read. The
# assertion is stable once the row exists.
> SELECT s.name, bool_and(u.snapshot_committed), sum(u.updates_staged) > 0
  FROM mz_internal.mz_source_statuses s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 't_paused'
  GROUP BY s.name
t_paused false true
