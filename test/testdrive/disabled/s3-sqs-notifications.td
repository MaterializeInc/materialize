# Copyright Materialize, Inc. All rights reserved.
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

$ set bucket=materialize-ci-testdrive-${testdrive.seed}

$ s3-create-bucket bucket=${bucket}

$ set queue-name=materialize-ci-notifications-${testdrive.seed}

$ s3-add-notifications bucket=${bucket} queue=${queue-name} sqs-validation-timeout=5m

$ s3-put-object bucket=${bucket} key=short/a
a1
a2
a3

$ s3-put-object bucket=${bucket} key=short/b
b1
b2
b3

$ set otherbucket=materialize-ci-td-other-${testdrive.seed}
$ set otherqueue=materialize-ci-other-${testdrive.seed}
$ s3-create-bucket bucket=${otherbucket}
$ s3-add-notifications bucket=${otherbucket} queue=${otherqueue} sqs-validation-timeout=5m

> CREATE MATERIALIZED SOURCE s3_scan_notifications
  FROM S3 DISCOVER OBJECTS MATCHING 'short/*'
  USING BUCKET SCAN '${bucket}', SQS NOTIFICATIONS '${queue-name}'
  WITH (
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

$ s3-put-object bucket=${bucket} key=short/d
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
$ s3-put-object bucket=${otherbucket} key=short/e
e1
e2
e3

> CREATE MATERIALIZED SOURCE s3_notifications
  FROM S3 DISCOVER OBJECTS MATCHING 'short/e' USING SQS NOTIFICATIONS '${otherqueue}'
  WITH (
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
