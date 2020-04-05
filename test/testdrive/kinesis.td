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

> CREATE SOURCE f
  FROM KINESIS ARN 'arn:aws:kinesis:${testdrive.aws-region}:${testdrive.aws-account}:stream/testdrive-test-${testdrive.seed}'
  WITH (access_key = '${testdrive.aws-access-key}',
        secret_access_key = '${testdrive.aws-secret-access-key}',
        token = '${testdrive.aws-token}',
        endpoint = '${testdrive.kinesis-endpoint}')
  FORMAT BYTES;

> CREATE MATERIALIZED VIEW f_view
  AS SELECT CONVERT_FROM(data, 'utf8') FROM f

> SELECT * FROM f_view
"here's a test string"
