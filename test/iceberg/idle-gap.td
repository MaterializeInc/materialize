# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A live idle -> data transition must not halt the Iceberg sink.
#
# The sink reads a stock differential `arrange_named` stream. When the input
# frontier advances over a range holding no data, that arrange emits nothing
# downstream but still advances its batcher's lower bound, so the next
# non-empty batch starts past the previous batch's upper. Tables tick their
# persist upper forward once per second with zero writes, so this gap is the
# steady state of any caught-up sink, not an edge case.
#
# A halt is invisible in the data: the restarted sink replays history densely,
# and dense replay is gapless, so it recovers and commits. Assert on health.

> CREATE SECRET IF NOT EXISTS idle_gap_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION idle_gap_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE idle_gap_src (a int, b text);

> INSERT INTO idle_gap_src VALUES (1, 'one');

> CREATE SINK idle_gap_sink
    FROM idle_gap_src
    INTO ICEBERG CATALOG CONNECTION idle_gap_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'idle_gap_table'
    )
    KEY (a) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ duckdb-execute name=idle_gap_iceberg
CREATE SECRET s3_secret_idle_gap (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# The snapshot has landed, so the sink is caught up and live from here on.
$ duckdb-query name=idle_gap_iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/idle_gap_table') ORDER BY a
1 one

# Go quiet, opening the gap, then write again.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

> INSERT INTO idle_gap_src VALUES (2, 'two')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

# Selecting the error text rather than counting it puts the message in the
# failure output.
> SELECT status, error FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'idle_gap_sink' AND error IS NOT NULL

# Row 2 lands with or without the defect. This only rules out a sink that
# stayed quiet because it stopped committing altogether.
$ duckdb-query name=idle_gap_iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/idle_gap_table') ORDER BY a
1 one
2 two
