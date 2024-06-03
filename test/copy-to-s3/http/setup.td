# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_to_expr = true;
ALTER SYSTEM SET max_sources = 50;
ALTER SYSTEM SET max_result_size = '100 GB';

> CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'

> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET aws_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );
