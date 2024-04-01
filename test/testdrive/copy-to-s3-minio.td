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

> CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'

> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET aws_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
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
> COPY t TO 's3://copytos3/test/1' || TO_CHAR(now(), 'YYYY-MM-DD')
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> SELECT a FROM t
1
2

> COPY (SELECT a FROM t) TO 's3://copytos3/test/2'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> COPY (SELECT array[1,2]::int[], false::bool, 'Inf'::double, '{"s": "abc"}'::jsonb, 1::mz_timestamp, 32767::smallint, 2147483647::integer, 9223372036854775807::bigint, 12345678901234567890123.4567890123456789::numeric(39,16), '2010-10-10'::date, '10:10:10'::time, '2010-10-10 10:10:10+00'::timestamp, '0 day'::interval, 'aaaa'::text, '\\xAAAA'::bytea, 'това е'::text, 'текст'::bytea) TO 's3://copytos3/test/3'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

! COPY (SELECT a FROM t) TO 's3://copytos3'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
contains:S3 bucket path is not empty

> COPY (SELECT * FROM generate_series(1, 1000000)) TO 's3://copytos3/test/4'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

$ set-from-sql var=key-1
SELECT 'test/1' || TO_CHAR(now(), 'YYYY-MM-DD')

$ s3-verify-data bucket=copytos3 key=${key-1}
1
2

$ s3-verify-data bucket=copytos3 key=test/2
1
2

$ s3-verify-data bucket=copytos3 key=test/3
"{1,2}",f,Infinity,"{""s"":""abc""}",1,32767,2147483647,9223372036854775807,12345678901234567890123.4567890123456789,2010-10-10,10:10:10,2010-10-10 10:10:10,00:00:00,aaaa,\x5c7841414141,това е,\xd182d0b5d0bad181d182
