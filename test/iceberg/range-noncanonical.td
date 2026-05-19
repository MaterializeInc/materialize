# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for database-issues#11330: COPY FROM PARQUET must canonicalize
# range values. External engines (here, DuckDB) author the Parquet file with
# non-canonical bounds for discrete ranges (e.g. `[1,10]` and `(5,15)` for
# int4range). MZ must rewrite these into its canonical half-open form on
# ingest, otherwise the resulting rows do not compare or hash equal to ranges
# constructed inside MZ.

> CREATE SECRET access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = 'http://${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true;

# Author the Parquet file directly in minio with DuckDB, so the bytes are not
# something MZ's own sink produced. The struct shape mirrors MZ's range
# encoding (lower/upper/lower_inclusive/upper_inclusive/empty).
$ duckdb-execute name=ranges
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
COPY (SELECT * FROM (VALUES (1, {'lower': 1::INTEGER, 'upper': 10::INTEGER, 'lower_inclusive': true, 'upper_inclusive': true, 'empty': false}), (2, {'lower': 5, 'upper': 15, 'lower_inclusive': false, 'upper_inclusive': false, 'empty': false})) t(id, r) ORDER BY id) TO 's3://test-bucket/noncanon_ranges/data.parquet' (FORMAT PARQUET);

> CREATE TABLE ranges(id int, r int4range);

> COPY INTO ranges FROM 's3://test-bucket/noncanon_ranges' (FORMAT PARQUET, AWS CONNECTION = aws_conn);

# DuckDB's `[1,10]` and `(5,15)` must arrive as MZ's canonical `[1,11)` and
# `[6,15)`.
> SELECT id, r::text FROM ranges ORDER BY id;
1 [1,11)
2 [6,15)

# Once canonicalized, the rows must compare equal to logically-identical
# values constructed inside MZ.
> SELECT r = '[1,10]'::int4range FROM ranges WHERE id = 1;
true

> SELECT r = '(5,15)'::int4range FROM ranges WHERE id = 2;
true
