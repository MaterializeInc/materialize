# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A source whose connection keeps failing restarts every few seconds. Neither
# it nor its table writes status rows for restarts that stall the same way, and
# `running` is written again once it recovers.

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (id int);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE PUBLICATION mz_source FOR TABLE t1;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> COMMIT

> SELECT * FROM t1;
1

> ALTER CONNECTION pg SET (HOST = 'mz-restart-loop.invalid') WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
stalled

$ set-from-sql var=status-rows
SELECT count(*)::text FROM mz_internal.mz_source_status_history
WHERE source_id IN (
    SELECT id FROM mz_sources WHERE name = 'mz_source'
    UNION ALL SELECT id FROM mz_tables WHERE name = 't1'
)

# Long enough for several restarts, one every
# `storage_suspend_and_restart_delay` (5 s).
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=25s

> SELECT count(*) FROM mz_internal.mz_source_status_history
  WHERE source_id IN (
      SELECT id FROM mz_sources WHERE name = 'mz_source'
      UNION ALL SELECT id FROM mz_tables WHERE name = 't1'
  )
${status-rows}

> ALTER CONNECTION pg SET (HOST = postgres) WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running
