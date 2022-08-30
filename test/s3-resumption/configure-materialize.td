# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET IF NOT EXISTS s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION IF NOT EXISTS s3_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

> CREATE SOURCE s3_text
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 's3/*.text'
  USING BUCKET SCAN 'testdrive-test-${testdrive.seed}',
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> CREATE SOURCE s3_gzip
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 's3/*.gzip'
  USING BUCKET SCAN 'testdrive-test-${testdrive.seed}',
  COMPRESSION GZIP
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;
