# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kinesis-create-stream stream=test

$ kinesis-ingest format=bytes stream=test
here's a test string

$ kinesis-verify stream=test
here's a test string

# Should just have the whole arn??
> CREATE SOURCE f
  FROM KINESIS ARN 'arn:aws:kinesis:${testdrive.kinesis-region}:000000000000:stream/testdrive-test-${testdrive.seed}'
  WITH (access_key = 'fake_access_key',
        secret_access_key = 'fake_secret_access_key',
        endpoint = 'http://localhost:4568')
  FORMAT BYTES;

> CREATE MATERIALIZED VIEW f_view
  AS SELECT CONVERT_FROM(data, 'utf8') FROM f

> SELECT * FROM f_view
"here's a test string"
