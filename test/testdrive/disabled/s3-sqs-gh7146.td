# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# gh7146: make sure that after dropping and recreating an S3 source
# and deleting and re-uploading all the s3 keys the data is in order.

$ s3-create-bucket bucket=philip-stoev-materialize
$ s3-add-notifications bucket=philip-stoev-materialize queue=philip-stoev-materialize sqs-validation-timeout=5m

> CREATE SOURCE s3
  FROM S3
  DISCOVER OBJECTS MATCHING '**/*.csv'
  USING BUCKET SCAN 'testdrive-philip-stoev-materialize-${testdrive.seed}',
  SQS NOTIFICATIONS 'testdrive-philip-stoev-materialize-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 3 COLUMNS;


> SELECT mz_internal.mz_sleep(10)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=1.csv
1,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=2.csv
2,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=3.csv
3,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=4.csv
4,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=5.csv
5,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=6.csv
6,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=7.csv
7,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=8.csv
8,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=9.csv
9,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=10.csv
10,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>


> DROP SOURCE s3;

$ s3-delete-objects bucket=philip-stoev-materialize
1.csv
2.csv
3.csv
4.csv
5.csv
6.csv
7.csv
8.csv
9.csv
10.csv

> SELECT mz_internal.mz_sleep(10)
<null>


> CREATE SOURCE s3
  FROM S3
  DISCOVER OBJECTS MATCHING '**/*.csv'
  USING BUCKET SCAN 'testdrive-philip-stoev-materialize-${testdrive.seed}',
  SQS NOTIFICATIONS 'testdrive-philip-stoev-materialize-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 3 COLUMNS;

> SELECT mz_internal.mz_sleep(40)
<null>

# We create 1 key that never existed before
$ s3-put-object bucket=philip-stoev-materialize key=0.csv
10,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=1.csv
11,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=2.csv
12,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=3.csv
13,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=4.csv
14,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=5.csv
15,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=6.csv
16,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=7.csv
17,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=8.csv
18,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=9.csv
19,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

$ s3-put-object bucket=philip-stoev-materialize key=10.csv
20,1, x

> SELECT mz_internal.mz_sleep(0.2)
<null>

# Another key that did not exist before
$ s3-put-object bucket=philip-stoev-materialize key=11.csv
21,1, x

> SELECT COUNT(*), MIN(column1), MAX(column1) FROM s3
12 10 21
