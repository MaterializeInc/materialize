# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs after a Materialize restart, following restart-nested-column.td. All
# three sinks resume against tables the catalog created with its own field
# ids. Before the fix for SS-522 the two sinks over the array-column relation
# stalled here with "Iceberg table '...' schema does not match expected
# schema" while the flat one resumed.

> SELECT name, status, error IS NULL FROM mz_internal.mz_sink_statuses
  WHERE name LIKE 'restart_%' ORDER BY name
restart_flat_append running true
restart_nested_append running true
restart_nested_upsert running true

# And they keep committing after the restart.
> INSERT INTO restart_nested VALUES (3, '{3}', 30);

> INSERT INTO restart_flat VALUES (2, 20);

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=restart_iceberg_verify
CREATE SECRET s3_secret_restart_verify (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=restart_iceberg_verify
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_nested_append')
3

$ duckdb-query name=restart_iceberg_verify
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_nested_upsert')
3

$ duckdb-query name=restart_iceberg_verify
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/restart_flat_append')
2
