# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT 1
1

$ set td-bucket=testdrive-test-${testdrive.seed}

$ s3-create-bucket bucket=test

$ s3-put-object bucket=test key=a
a1

> CREATE MATERIALIZED SOURCE s3_base_case
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_base_case
a1

# check using profiles at all
> CREATE MATERIALIZED SOURCE s3_profile_credentials
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials'
  )
  FORMAT TEXT;

> SELECT text FROM s3_profile_credentials
a1

# check switching to a role that does have access to this bucket
> CREATE MATERIALIZED SOURCE s3_role_allowed
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials',
    region = '${testdrive.aws-region}',
    role_arn = '${arg.allowed-role-arn}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_role_allowed
a1

# check inheriting credentials from another profile
> CREATE MATERIALIZED SOURCE s3_profile_allowed
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'allowed'
  )
  FORMAT TEXT;

> SELECT text FROM s3_profile_allowed
a1

# Check using a profile that does not itself configure a region, but references
# another profile which does.
> CREATE MATERIALIZED SOURCE s3_allowed_no_region
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'allowed-no-region'
  )
  FORMAT TEXT;

> SELECT text FROM s3_allowed_no_region
a1


# Error cases

# aws timeout is pretty long, so things just show up as pending but empty for awhile
$ set-sql-timeout duration=1m

! CREATE MATERIALIZED SOURCE s3_profile_no_region
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'no-region'
  )
  FORMAT TEXT;
contains: Unable to validate AWS credentials

> CREATE MATERIALIZED SOURCE s3_profile_denied
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'denied'
  )
  FORMAT TEXT;

! SELECT text FROM s3_profile_denied
regex: Unable to list S3 bucket.*Access Denied

# check credentials from profile, but using a role that denies access
> CREATE MATERIALIZED SOURCE s3_role_denied
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials',
    region = '${testdrive.aws-region}',
    role_arn = '${arg.denied-role-arn}'
  )
  FORMAT TEXT;

! SELECT text FROM s3_role_denied
regex: Unable to list S3 bucket.*Access Denied
