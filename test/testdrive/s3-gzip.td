# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test explicit lack of compression

$ s3-create-bucket bucket=no-compression

$ s3-put-object bucket=no-compression key=short/compressed
a1
a2
a3

$ s3-put-object bucket=no-compression key=short/uncompressed
b1
b2
b3

> CREATE SECRET s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION s3_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

> CREATE SOURCE s3_all_none
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS USING BUCKET SCAN 'testdrive-no-compression-${testdrive.seed}'
  COMPRESSION NONE
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_all_none
a1
a2
a3
b1
b2
b3

# Test explicit lack of compression

$ s3-create-bucket bucket=gzip-compression

# ensure that we can handle empty objects
$ s3-put-object bucket=gzip-compression key=short/

$ s3-put-object bucket=gzip-compression key=short/compressed compression=gzip
a1
a2
a3

$ s3-put-object bucket=gzip-compression key=short/uncompressed compression=gzip
b1
b2
b3

> CREATE SOURCE s3_all_gzip
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS USING BUCKET SCAN 'testdrive-gzip-compression-${testdrive.seed}'
  COMPRESSION GZIP
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_all_gzip
a1
a2
a3
b1
b2
b3

# Test automatic decompression handling
# $ set bucket=materialize-ci-testdrive-auto-${testdrive.seed}
#
# $ s3-create-bucket bucket=${bucket}
#
# $ s3-put-object bucket=${bucket} key=short/compressed compression=gzip
# a1
# a2
# a3
#
# $ s3-put-object bucket=${bucket} key=short/uncompressed
# b1
# b2
# b3
#
# > CREATE SOURCE s3_all_auto
#   FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${bucket}'
#   COMPRESSION AUTO
#   LEGACYWITH (
#     region = '${testdrive.aws-region}',
#     endpoint = '${testdrive.aws-endpoint}',
#     access_key_id = '${testdrive.aws-access-key-id}',
#     secret_access_key = '${testdrive.aws-secret-access-key}',
#     token = '${testdrive.aws-token}'
#   )
#   FORMAT TEXT;
#
# > SELECT * FROM s3_all
# a1
# a2
# a3
# b1
# b2
# b3
