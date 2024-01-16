# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_aws_connection = true;
ALTER SYSTEM SET enable_copy_to_s3 = true;
ALTER SYSTEM SET enable_connection_validation_syntax = true;

> CREATE CONNECTION aws_conn
  TO AWS (ASSUME ROLE ARN 'assume-role', ASSUME ROLE SESSION NAME 'session-name');

> CREATE TABLE t (a int);

> INSERT INTO t VALUES (1);

> COPY t TO 's3://hello/' || mz_now()
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv'
  );
