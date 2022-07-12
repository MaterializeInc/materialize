# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These tests are flaky: S3 has no guarantees about how long it takes for SQS notifications to get
# set up. Even waiting 5 minutes isn't long enough to get failures down to below once a week.
#
# https://github.com/MaterializeInc/materialize/issues/6355

# gh 7182: Ensure that messages are not lost after dropping and recreating an S3 source that
# listens to an SQS queue


$ set buk=mz-sqs-tst
$ s3-create-bucket bucket=${buk}
$ s3-add-notifications bucket=${buk} queue=${buk} sqs-validation-timeout=5m

> CREATE SOURCE s3
  FROM S3
  DISCOVER OBJECTS MATCHING '**/*.csv' USING
  SQS NOTIFICATIONS 'testdrive-${buk}-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> DROP SOURCE s3;

> CREATE SOURCE s3
  FROM S3
  DISCOVER OBJECTS MATCHING '**/*.csv' USING
  SQS NOTIFICATIONS 'testdrive-${buk}-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

$ s3-put-object bucket=${buk} key=0.csv
0

$ s3-put-object bucket=${buk} key=1.csv
1

$ s3-put-object bucket=${buk} key=2.csv
2

> SELECT text FROM s3
0
1
2
