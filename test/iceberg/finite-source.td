# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A fresh Iceberg sink whose input contains data and then finishes (its
# frontier closes) must commit all the data and then seal itself with a
# final batch whose upper is the empty frontier.

> CREATE SECRET finite_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION finite_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# A finite COUNTER generator: value N is emitted at offset N-1, offsets
# <= AS OF collapse into a single snapshot timestamp, and offsets >= UP TO
# are dropped. AS OF 99, UP TO 100 emits the values 1..100 at one
# timestamp and then closes the input's frontier.
> CREATE SOURCE finite_src FROM LOAD GENERATOR COUNTER (AS OF 99, UP TO 100);

> CREATE TABLE finite_tbl FROM SOURCE finite_src;

# Wait until the data has landed and the generator has terminated before
# creating the sink, so the sink sees an input that is already closed.
> SELECT count(*) FROM finite_tbl
100

> CREATE SINK finite_demo
    FROM finite_tbl
    INTO ICEBERG CATALOG CONNECTION finite_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'finite_source_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# The sink must not stall or error.
> SELECT error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'finite_demo'
true

# Iceberg sinks commit asynchronously; wait for the commits to land.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-execute name=finite_iceberg
CREATE SECRET s3_secret_finite (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# Two commits: the snapshot batch carrying the data, then the final batch
# that seals the sink.
$ duckdb-query name=finite_iceberg
SELECT count(*) = 2 FROM iceberg_snapshots('s3://test-bucket/default_namespace/finite_source_table')
true

# All 100 rows must be present.
$ duckdb-query name=finite_iceberg
SELECT count(*), min(counter), max(counter) FROM iceberg_scan('s3://test-bucket/default_namespace/finite_source_table')
100 1 100
