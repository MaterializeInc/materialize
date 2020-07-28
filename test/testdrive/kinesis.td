# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kinesis-create-stream stream=test shards=1

$ kinesis-ingest format=bytes stream=test
here's a test string

$ kinesis-verify stream=test
here's a test string

$ kinesis-ingest format=bytes stream=test
here's a second test string

$ kinesis-verify stream=test
here's a test string
here's a second test string

! CREATE SOURCE custom_source
  FROM KINESIS ARN 'arn:aws:kinesis:custom-region::stream/fake-stream'
  WITH (access_key_id = 'fake_access_key_id',
        secret_access_key = 'fake_secret_access_key')
  FORMAT BYTES;
If providing a custom region, an `endpoint` option must also be provided

> CREATE SOURCE f
  FROM KINESIS ARN 'arn:aws:kinesis:${testdrive.aws-region}:${testdrive.aws-account}:stream/testdrive-test-${testdrive.seed}'
  WITH (access_key_id = '${testdrive.aws-access-key-id}',
        secret_access_key = '${testdrive.aws-secret-access-key}',
        token = '${testdrive.aws-token}',
        endpoint = '${testdrive.aws-endpoint}')
  FORMAT BYTES;

> CREATE MATERIALIZED VIEW f_view
  AS SELECT CONVERT_FROM(data, 'utf8') FROM f

> SELECT * FROM f_view
"here's a test string"
"here's a second test string"
