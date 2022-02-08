# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that materialized --aws-external-id ID allows connecting

$ set td-bucket=testdrive-witheid-${testdrive.seed}

$ s3-create-bucket bucket=witheid

$ s3-put-object bucket=witheid key=a
a1

# check using profiles at all
> CREATE MATERIALIZED SOURCE allowed_eid
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN '${td-bucket}'
  WITH (
    profile = 'credentials',
    role_arn = '${arg.role-requires-eid}'
  )
  FORMAT TEXT;

> SELECT text FROM allowed_eid
a1
