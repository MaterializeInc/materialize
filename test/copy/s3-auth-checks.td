# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Auth related for COPY TO expr.

# COPY TO expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_to_expr = true;

# There are 3 users with different permissions policies to validate:
# User 'readwritedelete': PutObject, ListBucket, DeleteObject
# User 'nodelete': PutObject, ListBucket
# User 'read': GetObject, ListBucket

### Set up connections - one for each user

> CREATE SECRET user_readwritedelete_secret AS '${arg.s3-user-readwritedelete-secret-key}'
> CREATE SECRET user_nodelete_secret AS '${arg.s3-user-nodelete-secret-key}'
> CREATE SECRET user_read_secret AS '${arg.s3-user-read-secret-key}'

> CREATE CONNECTION user_readwritedelete_aws_conn
  TO AWS (
    ACCESS KEY ID = 'readwritedelete',
    SECRET ACCESS KEY = SECRET user_readwritedelete_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION user_nodelete_aws_conn
  TO AWS (
    ACCESS KEY ID = 'nodelete',
    SECRET ACCESS KEY = SECRET user_nodelete_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION user_read_aws_conn
  TO AWS (
    ACCESS KEY ID = 'read',
    SECRET ACCESS KEY = SECRET user_read_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

### End setup

# read user should fail immediately
! COPY (SELECT generate_series(1, 100)) TO 's3://copytos3/auth_test'
  WITH (
    AWS CONNECTION = user_read_aws_conn,
    FORMAT = 'csv'
  );
contains:AccessDenied

# nodelete user should fail immediately
! COPY (SELECT generate_series(1, 100)) TO 's3://copytos3/auth_test'
  WITH (
    AWS CONNECTION = user_nodelete_aws_conn,
    FORMAT = 'csv'
  );
contains:AccessDenied

# user with readwritedelete permissions should succeed copying into the same
# path which indicates that the previous permissions failures did not
# write anything to the S3 path at all and corrupt it
> COPY (SELECT generate_series(1, 100)) TO 's3://copytos3/auth_test'
  WITH (
    AWS CONNECTION = user_readwritedelete_aws_conn,
    FORMAT = 'csv'
  );


# Test broken AWS connection error message at copy time

> CREATE CONNECTION broken_aws_conn
  TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::400121260767:role/MZS3Exporter',
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

! COPY (SELECT generate_series(1, 100)) TO 's3://copytos3/auth_test'
  WITH (
    AWS CONNECTION = broken_aws_conn,
    FORMAT = 'csv'
  );
contains: dispatch failure: other: an error occurred while loading credentials
