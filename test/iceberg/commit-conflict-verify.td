# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that all 43 rows (3 initial + 40 inserted during the conflict
# window) are present in the Iceberg table. DuckDB's iceberg_scan reads
# actual manifest files, so it sees exactly what was committed.

$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=30s

$ duckdb-query name=iceberg
SELECT COUNT(*) FROM iceberg_scan('s3://test-bucket/default_namespace/conflict_table')
43
