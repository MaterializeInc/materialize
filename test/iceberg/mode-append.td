# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for MODE APPEND on Iceberg sinks.
#
# In append mode, every update is written as a data row with two extra columns:
#   _mz_diff      (int)    -- +1 for insertions, -1 for deletions
#   _mz_timestamp (bigint) -- the Materialize logical timestamp of the update
#
# Unlike MODE UPSERT, deletions are NOT written as Iceberg delete files;
# they appear as ordinary data rows with _mz_diff = -1.

> CREATE SECRET IF NOT EXISTS append_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION IF NOT EXISTS append_aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET append_access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION append_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE append_src (a int, b text);

> INSERT INTO append_src VALUES (1, 'one'), (2, 'two'), (3, 'three');

# KEY is not required (or permitted) for MODE APPEND
> CREATE SINK append_demo
    FROM append_src
    INTO ICEBERG CATALOG CONNECTION append_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'append_demo_table'
    )
    USING AWS CONNECTION append_aws_conn
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously; wait for at least one commit interval to elapse
# before querying the table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=append_iceberg
CREATE SECRET s3_secret_append (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# Verify initial inserts are written as _mz_diff=1 rows, ordered by timestamp then row.
$ duckdb-query name=append_iceberg
SELECT a, b, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/append_demo_table') ORDER BY _mz_timestamp, a, b, _mz_diff
1 one 1
2 two 1
3 three 1

$ duckdb-query name=append_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/append_demo_table') WHERE _mz_timestamp IS NOT NULL
3

# Test delete: should produce a _mz_diff=-1 row, NOT an Iceberg delete file.
> DELETE FROM append_src WHERE a = 2;

# Wait for the delete to be committed to the Iceberg table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=append_iceberg
SELECT a, b, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/append_demo_table') ORDER BY _mz_timestamp, a, b, _mz_diff
1 one 1
2 two 1
3 three 1
2 two -1

# Test update: should produce a _mz_diff=-1 row for the old value and _mz_diff=+1 for the new value,
# both at the same timestamp.
> UPDATE append_src SET b = 'ONE' WHERE a = 1;

# Wait for the update to be committed to the Iceberg table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=append_iceberg
SELECT a, b, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/append_demo_table') ORDER BY _mz_timestamp, a, b, _mz_diff
1 one 1
2 two 1
3 three 1
2 two -1
1 ONE 1
1 one -1

# Verify via the Polaris REST catalog as well.
$ duckdb-execute name=append_iceberg
CREATE SECRET polaris_secret_append (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris (TYPE iceberg, SECRET polaris_secret_append, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=append_iceberg
SELECT a, b, _mz_diff FROM polaris.default_namespace.append_demo_table ORDER BY _mz_timestamp, a, b, _mz_diff
1 one 1
2 two 1
3 three 1
2 two -1
1 ONE 1
1 one -1

# Test: table with columns named "_mz_diff" and "_mz_timestamp" — these clash with
# the columns that MODE APPEND automatically appends to the Iceberg schema.

> CREATE TABLE append_clash_src (id int, _mz_diff int);

! CREATE SINK append_clash_demo
    FROM append_clash_src
    INTO ICEBERG CATALOG CONNECTION append_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'append_clash_table'
    )
    USING AWS CONNECTION append_aws_conn
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');
contains:conflicts with the system column that MODE APPEND adds to the Iceberg table

> CREATE TABLE append_clash_src_1 (id int, _mz_timestamp bigint);

! CREATE SINK append_clash_demo_1
    FROM append_clash_src_1
    INTO ICEBERG CATALOG CONNECTION append_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'append_clash_table_1'
    )
    USING AWS CONNECTION append_aws_conn
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');
contains:conflicts with the system column that MODE APPEND adds to the Iceberg table
