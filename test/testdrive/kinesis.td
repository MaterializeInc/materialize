# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kinesis-create-stream stream=test shards=1

$ kinesis-ingest format=bytes stream=test
here is a test string

$ kinesis-verify stream=test
here is a test string

$ kinesis-ingest format=bytes stream=test
here is a second test string

$ kinesis-verify stream=test
here is a test string
here is a second test string

> CREATE SECRET kinesis_bad_conn_secret AS 'fake_secret_access_key';

> CREATE CONNECTION kinesis_bad_conn FOR AWS
  ACCESS KEY ID = 'fake_access_key_id',
  SECRET ACCESS KEY = SECRET kinesis_bad_conn_secret;

! CREATE SOURCE custom_source
  FROM KINESIS CONNECTION kinesis_bad_conn
  ARN 'arn:aws:kinesis:custom-region::stream/fake-stream'
  FORMAT BYTES;
contains:dns error: failed to lookup address information: Name or service not known

> CREATE SECRET kinesis_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION kinesis_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET kinesis_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

> CREATE SOURCE f
  FROM KINESIS CONNECTION kinesis_conn
  ARN 'arn:aws:kinesis:${testdrive.aws-region}:${testdrive.aws-account}:stream/testdrive-test-${testdrive.seed}'
  LEGACYWITH (endpoint = '${testdrive.aws-endpoint}')
  FORMAT BYTES;

> CREATE MATERIALIZED VIEW f_view
  AS SELECT CONVERT_FROM(data, 'utf8') FROM f

> SELECT * FROM f_view
"here is a test string"
"here is a second test string"
