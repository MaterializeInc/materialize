# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for COPY TO expr.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_aws_connection = true;
ALTER SYSTEM SET enable_copy_to_expr = true;

# Prepare table data
> CREATE TABLE t (a int);
> INSERT INTO t VALUES (1);
> INSERT INTO t VALUES (2);

> CREATE SECRET aws_secret_access_key as '${arg.secret-key}';
> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.access-key}',
    SECRET ACCESS KEY = SECRET aws_secret_access_key,
    ENDPOINT = '${arg.endpoint}',
    REGION = '${arg.region}'
  );

! COPY t TO 's3://path/to/dir';
contains:AWS CONNECTION is required for COPY ... TO <expr>

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn
  );
contains:only CSV format is supported for COPY ... TO <expr>

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'binary'
  );
contains:only CSV format is supported for COPY ... TO <expr>

! COPY t TO '/path/'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv'
  );
contains:only 's3://...' urls are supported as COPY TO target

! COPY t TO NULL
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv'
  );
contains:COPY TO target value can not be null

! COPY t TO 1234
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv'
  );
contains:COPY TO target must have type text, not type integer

! COPY (SELECT * FROM t ORDER BY 1) TO NULL
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv'
  );
contains:ORDER BY is not supported in SELECT query for COPY statements

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv',
    MAX FILE SIZE = '1kB'
  );
contains:MAX FILE SIZE cannot be less than 16MB

# Creating cluster with multiple replicas, each with multiple workers
> CREATE CLUSTER c1 REPLICAS (r1 (size '2'), r2 (size '2'));
> SET cluster = c1;

# functions like now() should work in the s3 path
> COPY t TO 's3://${arg.s3-prefix}/1/' || TO_CHAR(now(), 'YYYY-MM-DD')
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> COPY (SELECT a FROM t) TO 's3://${arg.s3-prefix}/2/'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> COPY (SELECT 1000) TO 's3://${arg.s3-prefix}/3/'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> DROP CLUSTER c1 CASCADE;
