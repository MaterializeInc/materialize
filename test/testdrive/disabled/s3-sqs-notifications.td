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

$ s3-create-bucket bucket=test

$ s3-add-notifications bucket=test queue=test sqs-validation-timeout=5m

$ s3-put-object bucket=test key=short/a
a1
a2
a3

$ s3-put-object bucket=test key=short/b
b1
b2
b3

$ s3-create-bucket bucket=other
$ s3-add-notifications bucket=other queue=other sqs-validation-timeout=5m

> CREATE SOURCE s3_scan_notifications
  FROM S3 DISCOVER OBJECTS MATCHING 'short/*'
  USING BUCKET SCAN 'testdrive-test-${testdrive.seed}', SQS NOTIFICATIONS 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_scan_notifications ORDER BY text;
a1
a2
a3
b1
b2
b3

$ s3-put-object bucket=test key=short/d
d

> SELECT text FROM s3_scan_notifications ORDER BY text;
a1
a2
a3
b1
b2
b3
d

# ensure that only the source we care about gets the notifications
> DROP SOURCE s3_scan_notifications

# check just pulling SQS

# This section fails in localstack, but succeeds in AWS/CI
$ s3-put-object bucket=other key=short/e
e1
e2
e3

> CREATE SOURCE s3_notifications
  FROM S3 DISCOVER OBJECTS MATCHING 'short/e' USING SQS NOTIFICATIONS 'testdrive-other-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_notifications ORDER BY text;
e1
e2
e3
