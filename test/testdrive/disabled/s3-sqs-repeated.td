# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These tests are flaky and take a while to ensure a timeout inside of
# materialized is reached, both of which need to be resolved before this can be
# enabled.
#
# Flakiness documented here: https://github.com/MaterializeInc/materialize/issues/6355
#
# We'd also want to be able to explicitly set the retry timeout inside this
# file instead of relying on an arbitrary number, which probably depends on
# https://github.com/MaterializeInc/materialize/issues/7115

> DROP SOURCE IF EXISTS s3_source

$ s3-create-bucket bucket=sqs-repeated
$ s3-add-notifications bucket=sqs-repeated queue=sqs-repeated sqs-validation-timeout=5m

$ s3-put-object bucket=sqs-repeated key=1
1

# give aws time to propagate the mssages
> SELECT mz_internal.mz_sleep(10)
<null>

$ s3-delete-objects bucket=sqs-repeated
1

> CREATE SOURCE s3_source
  FROM S3
  DISCOVER OBJECTS USING
  SQS NOTIFICATIONS 'testdrive-sqs-repeated-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

# Give it time to error read objects that are in the queue but don't exist
#
# The default retry timeout for object downloads is 30 seconds
> SELECT mz_internal.mz_sleep(40)
<null>

$ s3-put-object bucket=sqs-repeated key=1
1

$ s3-put-object bucket=sqs-repeated key=2
2

$ s3-put-object bucket=sqs-repeated key=3
3

$ s3-put-object bucket=sqs-repeated key=4
4

$ s3-put-object bucket=sqs-repeated key=5
5

$ s3-put-object bucket=sqs-repeated key=6
6

$ s3-put-object bucket=sqs-repeated key=7
7

$ s3-put-object bucket=sqs-repeated key=8
8

$ s3-put-object bucket=sqs-repeated key=9
9

$ s3-put-object bucket=sqs-repeated key=10
10

> SELECT count(*) FROM s3_source
10

> DROP SOURCE s3_source
