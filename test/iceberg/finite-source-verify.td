# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs after a Materialize restart, following finite-source.td. The sink
# resumes from an Iceberg table whose committed frontier is empty. It must
# come back healthy and must not commit anything new.

> SELECT error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'finite_demo'
true

# Give the restarted sink time to make any (incorrect) further commits.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-execute name=finite_iceberg_verify
CREATE SECRET s3_secret_finite_verify (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# Still exactly the two original commits.
$ duckdb-query name=finite_iceberg_verify
SELECT count(*) = 2 FROM iceberg_snapshots('s3://test-bucket/default_namespace/finite_source_table')
true

# And the data is unchanged.
$ duckdb-query name=finite_iceberg_verify
SELECT count(*), min(counter), max(counter) FROM iceberg_scan('s3://test-bucket/default_namespace/finite_source_table')
100 1 100
