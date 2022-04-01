# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# test some things that should succeed, even if the mz instance started off
# without an aws config file.

$ set td-bucket=testdrive-testrestart-${testdrive.seed}


$ s3-put-object bucket=testrestart key=a
a1

# check using profiles at all
> CREATE MATERIALIZED SOURCE s3_explicit_credentials
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials'
  )
  FORMAT TEXT;

> SELECT text FROM s3_explicit_credentials
a1

# check switching to a role that does have access to this bucket
> CREATE MATERIALIZED SOURCE s3_allowed_by_role
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials',
    region = '${testdrive.aws-region}',
    role_arn = '${arg.allowed-role-arn}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_allowed_by_role
a1
