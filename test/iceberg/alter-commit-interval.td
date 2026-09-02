# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ALTER SINK ... SET (COMMIT INTERVAL ...) changes the commit interval of a
# running Iceberg sink. The sink dataflow restarts with the new interval and
# subsequent batches follow the new cadence.

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE t (a int, b text);

> INSERT INTO t VALUES (1, 'one'), (2, 'two');

> CREATE SINK alter_demo
    FROM t
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'alter_ci_table'
    )
    KEY (a) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Only COMMIT INTERVAL is alterable.
! ALTER SINK alter_demo SET (SNAPSHOT = false)
contains: ALTER SINK ... SET (SNAPSHOT) not yet supported

! ALTER SINK alter_demo RESET (SNAPSHOT)
contains: ALTER SINK ... RESET (SNAPSHOT) not yet supported

# Iceberg sinks require a commit interval, so it cannot be reset.
! ALTER SINK alter_demo RESET (COMMIT INTERVAL)
contains: Iceberg sink must specify COMMIT INTERVAL

# A sub-second interval would truncate to milliseconds (and possibly to a
# zero-width batch that never commits anything).
! ALTER SINK alter_demo SET (COMMIT INTERVAL = '500ms')
contains: COMMIT INTERVAL must be at least 1 second

! ALTER SINK alter_demo SET (COMMIT INTERVAL = '0s')
contains: COMMIT INTERVAL must be at least 1 second

# Iceberg sinks commit data asynchronously, need to wait for at least one
# commit interval.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/alter_ci_table') ORDER BY a
1 one
2 two

# Stretch the commit interval to an hour.
$ set-from-sql var=status_count
SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'alter_demo' AND status = 'running'

> ALTER SINK alter_demo SET (COMMIT INTERVAL = '1h')

> SELECT create_sql LIKE '%COMMIT INTERVAL = ''1h''%' FROM (SHOW CREATE SINK alter_demo)
true

# Wait for the restarted sink dataflow to reach running.
> SELECT COUNT(*) > ${status_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'alter_demo' AND status = 'running'
true

# The restarted sink picks a cutoff time: rows older than the cutoff are
# committed immediately (ignoring the interval), rows newer than it wait for
# the next hourly commit. We want row 3 to wait, so it must be inserted after
# the cutoff is picked.
#
# The problem: the sink picks the cutoff only after loading the Iceberg table
# from the catalog, which takes a few network round trips, and nothing
# queryable tells us when that has happened. The 'running' status above
# appears before it. So we sleep and assume 5 seconds is enough.
#
# If row 3 ever shows up in the "1 one / 2 two" check below, the table load
# took longer than the sleep. Bump the duration.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# Data inserted from now on sits in an open one-hour batch and must not show
# up in the Iceberg table.
> INSERT INTO t VALUES (3, 'three')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/alter_ci_table') ORDER BY a
1 one
2 two

# Shrink the interval back down. This also re-plans the create_sql that the
# previous ALTER rewrote. The pending row must now commit promptly.
> ALTER SINK alter_demo SET (COMMIT INTERVAL = '1s')

> SELECT create_sql LIKE '%COMMIT INTERVAL = ''1s''%' FROM (SHOW CREATE SINK alter_demo)
true

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/alter_ci_table') ORDER BY a
1 one
2 two
3 three

# The sink is healthy after all the alters.
> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'alter_demo'
running

# Setting the commit interval to its current value is a no-op and does not
# restart the sink dataflow, so no new status history entries appear.
$ set-from-sql var=noop_status_count
SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'alter_demo'

> ALTER SINK alter_demo SET (COMMIT INTERVAL = '1s')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> SELECT COUNT(*) = ${noop_status_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'alter_demo'
true
