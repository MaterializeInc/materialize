# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ s3-create-bucket bucket=test

$ s3-put-object bucket=test key=posix
foo
bar

> CREATE SECRET s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION s3_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

> CREATE SOURCE posix
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'posix' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT BYTES;

> SELECT data FROM posix
foo
bar

$ s3-put-object bucket=test key=non-posix trailing-newline=false
foo
bar

> CREATE SOURCE non_posix
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'non-posix' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT BYTES;

> SELECT data FROM non_posix
foo
bar
