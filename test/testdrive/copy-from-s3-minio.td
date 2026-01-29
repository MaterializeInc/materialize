# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for COPY FROM expr.

# COPY FROM expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true;


# Prepare the table we want to COPY INTO.
> CREATE TABLE t1 (a text, b text);
> CREATE TABLE t1_num (a text, b numeric);
> CREATE TABLE t1_bad_cast (a jsonb, b int);
> CREATE TABLE t1_single_col (a text);
> CREATE TABLE t1_not_null (a text, b int, c float NOT NULL);
> CREATE TABLE t1_fun_house (y int, z text, c float NOT NULL DEFAULT 1.11, d bigint);

$ s3-file-upload bucket=copytos3 key=csv/1.csv repeat=2
none,100

$ s3-set-presigned-url bucket=copytos3 key=csv/1.csv var-name=1_csv_url

# everything imports as text
> COPY INTO t1 FROM '${1_csv_url}' (FORMAT CSV);
> SELECT * FROM t1;
none 100
none 100

# columns can be cast to types
> COPY INTO t1_num FROM '${1_csv_url}' (FORMAT CSV);
> SELECT *, pg_typeof(b) FROM t1_num;
none 100 numeric
none 100 numeric

# FYI: casting a number to json/jsonb, e.g. '1'::jsonb, is valid, casting alphabetic string is not
! COPY INTO t1_bad_cast FROM '${1_csv_url}' (FORMAT CSV);
contains:failed to decode Row

! COPY INTO t1_single_col FROM '${1_csv_url}' (FORMAT CSV);
contains:wrong number of columns

# disabled as MZ panics database-issues#9886
# ! COPY INTO t1_not_null FROM '${1_csv_url}' (FORMAT CSV);
# contains:some error that's not a panic

> COPY INTO t1_fun_house (z, y) FROM '${1_csv_url}' (FORMAT CSV);
> SELECT * from t1_fun_house;
100 none 1.11 <null>
100 none 1.11 <null>

# gzip Compression.

$ s3-file-upload bucket=copytos3 key=csv/2.csv.gz repeat=2 compression=gzip
gzip,200

$ s3-set-presigned-url bucket=copytos3 key=csv/2.csv.gz var-name=2_csv_url

> COPY INTO t1 FROM '${2_csv_url}' (FORMAT CSV);

> SELECT * FROM t1;
gzip 200
gzip 200
none 100
none 100

# bzip2 Compression.

$ s3-file-upload bucket=copytos3 key=csv/3.csv.bz2 repeat=2 compression=bzip2
bzip2,300

$ s3-set-presigned-url bucket=copytos3 key=csv/3.csv.bz2 var-name=3_csv_url

> COPY INTO t1 FROM '${3_csv_url}' (FORMAT CSV);

> SELECT * FROM t1;
bzip2 300
bzip2 300
gzip 200
gzip 200
none 100
none 100

# xz Compression.

$ s3-file-upload bucket=copytos3 key=csv/4.csv.xz repeat=2 compression=xz
xz,400

$ s3-set-presigned-url bucket=copytos3 key=csv/4.csv.xz var-name=4_csv_url

> COPY INTO t1 FROM '${4_csv_url}' (FORMAT CSV);

> SELECT * FROM t1;
bzip2 300
bzip2 300
gzip 200
gzip 200
none 100
none 100
xz 400
xz 400

# zstd Compression.

$ s3-file-upload bucket=copytos3 key=csv/5.csv.zst repeat=2 compression=zstd
zstd,500

$ s3-set-presigned-url bucket=copytos3 key=csv/5.csv.zst var-name=5_csv_url

> COPY INTO t1 FROM '${5_csv_url}' (FORMAT CSV);

> SELECT * FROM t1;
bzip2 300
bzip2 300
gzip 200
gzip 200
none 100
none 100
xz 400
xz 400
zstd 500
zstd 500

# Map and Project.

> CREATE TABLE t2 (a text DEFAULT 'hello', b text, c text);

$ s3-file-upload bucket=copytos3 key=default_vals.csv repeat=5
world

$ s3-set-presigned-url bucket=copytos3 key=default_vals.csv var-name=default_vals_csv_url

> COPY INTO t2 (b) FROM '${default_vals_csv_url}' (FORMAT CSV);

> SELECT * FROM t2;
hello world <null>
hello world <null>
hello world <null>
hello world <null>
hello world <null>

# Test the AWS Source.

> CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'

> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET aws_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

# Test glob patterns.

> CREATE TABLE t3 (a text, b text);

> COPY INTO t3 FROM 's3://copytos3' (FORMAT CSV, AWS CONNECTION = aws_conn, PATTERN = "csv/**");

> SELECT * FROM t3;
bzip2 300
bzip2 300
gzip 200
gzip 200
none 100
none 100
xz 400
xz 400
zstd 500
zstd 500

# Test explicit file lists.

> CREATE TABLE t4 (a text, b text);

> COPY INTO t4 FROM 's3://copytos3/csv' (FORMAT CSV, AWS CONNECTION = aws_conn, FILES = ["csv/1.csv"]);

> SELECT * FROM t4;
none 100
none 100
