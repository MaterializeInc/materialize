# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that a missing external ID cli parameter refuses to allow us to assume a role

$ set td-bucket=testdrive-noeid-${testdrive.seed}

$ s3-create-bucket bucket=noeid

$ s3-put-object bucket=noeid key=a
a1

! CREATE MATERIALIZED SOURCE s3_explicit_credentials
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials',
    role_arn = '${arg.role-requires-eid}'
  )
  FORMAT TEXT;
regex: Unable to validate AWS credentials:.*AccessDenied
