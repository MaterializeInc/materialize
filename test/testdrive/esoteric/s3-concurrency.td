# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ s3-create-bucket bucket=concurrency

$ set csv-rows=1000000
$ set csv-value=100
$ set object-count=50

$ s3-put-objects bucket=concurrency key=aOBJECT_ID repeat-contents=${csv-rows} object-count=${object-count}
keyOBJECT_ID,${csv-value}

> CREATE MATERIALIZED SOURCE s3_all_concurrent (key, value)
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN 'testdrive-concurrency-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 2 COLUMNS;

$ set-sql-timeout duration=10m

> SELECT sum(value::int) = ${csv-rows} * ${csv-value} * ${object-count} FROM s3_all_concurrent;
true

# > SELECT count(value) FROM s3_all_concurrent
# 10000000

