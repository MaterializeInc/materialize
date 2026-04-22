# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for database-issues#11326: upsert sinks that process
# more than DEFAULT_MAX_SEEN_ROWS (100k) distinct keys in one commit
# used to silently duplicate rows. Once the DeltaWriter evicted a row
# from seen_rows, subsequent deletes for that row fell back to equality
# deletes, which only apply to prior snapshots (lower sequence number)
# and so have no effect on the inserts in the same snapshot.
#
# A long COMMIT INTERVAL forces the bulk insert and the follow-up
# update to land in a single DeltaWriter session.

> CREATE SECRET access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE large_upsert_src (key INT, val TEXT);

> CREATE SINK large_upsert_sink
    FROM large_upsert_src
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'large_upsert_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (key) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '120s');

> INSERT INTO large_upsert_src
    SELECT generate_series, 'initial_' || generate_series::text
    FROM generate_series(1, 110000);

> UPDATE large_upsert_src
    SET val = 'updated_' || key::text
    WHERE key <= 100;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=150s

$ duckdb-execute name=iceberg
CREATE SECRET s3_sec (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=iceberg
SELECT COUNT(*) FROM iceberg_scan('s3://test-bucket/default_namespace/large_upsert_table')
110000

$ duckdb-query name=iceberg
SELECT COUNT(*) FROM iceberg_scan('s3://test-bucket/default_namespace/large_upsert_table') WHERE val LIKE 'updated_%'
100

$ duckdb-query name=iceberg
SELECT COUNT(*) FROM iceberg_scan('s3://test-bucket/default_namespace/large_upsert_table') WHERE val LIKE 'initial_%'
109900
