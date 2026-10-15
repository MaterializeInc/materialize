# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for SS-522: an Iceberg sink over a relation with an array
# column must survive a Materialize restart.
#
# The REST catalog assigns its own field ids when it creates the table,
# numbering a struct's direct children before any nested field, while the sink
# numbers depth-first. The table it creates for `restart_nested` therefore
# carries different ids for `a.items`, `a.dimensions` and `a.items.element`
# than the schema the sink derives. Writing works, because the writer uses the
# table's own schema; on resume the sink compares the two schemas and must not
# reject a table that differs only in ids. The flat relation is the control:
# its ids coincide under both schemes.
#
# This file runs before the restart; restart-nested-column-verify.td after.

> CREATE SECRET IF NOT EXISTS restart_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION restart_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE restart_nested (id bigint, a bigint[], w bigint);

> INSERT INTO restart_nested VALUES (1, '{1,2}', 10), (2, NULL, 20);

> CREATE TABLE restart_flat (id bigint, w bigint);

> INSERT INTO restart_flat VALUES (1, 10);

> CREATE SINK restart_nested_append
    FROM restart_nested
    INTO ICEBERG CATALOG CONNECTION restart_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'restart_nested_append'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> CREATE SINK restart_nested_upsert
    FROM restart_nested
    INTO ICEBERG CATALOG CONNECTION restart_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'restart_nested_upsert'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

> CREATE SINK restart_flat_append
    FROM restart_flat
    INTO ICEBERG CATALOG CONNECTION restart_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'restart_flat_append'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT name, status FROM mz_internal.mz_sink_statuses
  WHERE name LIKE 'restart_%' ORDER BY name
restart_flat_append running
restart_nested_append running
restart_nested_upsert running

# Iceberg sinks commit asynchronously; let the first commit interval pass.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=restart_iceberg
CREATE SECRET s3_secret_restart (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=restart_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_nested_append')
2

$ duckdb-query name=restart_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_nested_upsert')
2

$ duckdb-query name=restart_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_flat_append')
1
