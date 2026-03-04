# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for COPY FROM expr for parquet format.

# COPY FROM expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true;

> CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'

> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET aws_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

$ s3-upload-parquet-types bucket=copyfroms3 key-prefix=parquet/types.parquet

# Create a custom type for a composite type
> CREATE TYPE record_type AS (name text, age int, avg double precision);

# Prepare the table we want to COPY INTO.
> CREATE TABLE types (
  int8_col smallint,
  uint8_col uint2,
  int16_col smallint,
  uint16_col uint2,
  int32_col int,
  uint32_col uint4,
  int64_col bigint,
  uint64_col uint8,
  float32_col real,
  float64_col double precision,
  bool_col boolean,
  string_col text,
  binary_col bytea,
  date32_col date,
  timestamp_ms_col timestamp,
  -- timestampz_ms_col timestamp with time zone, -- not supported
  time32_col time,
  list_col bigint list,
  decimal_col decimal,
  json_col jsonb,
  record_col record_type,
  uuid_col uuid
  );

$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet var-name=types_url

> COPY INTO types FROM '${types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0


# gzip Compression.
$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet.gzip var-name=gzip_types_url

> COPY INTO types FROM '${gzip_types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0

# snappy Compression.
$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet.snappy var-name=snappy_types_url

> COPY INTO types FROM '${snappy_types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0

# brotli Compression.
$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet.brotli var-name=brotli_types_url

> COPY INTO types FROM '${brotli_types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0

# zstd Compression.
$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet.zstd var-name=zstd_types_url

> COPY INTO types FROM '${zstd_types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0

# lz4 Compression.
$ s3-set-presigned-url bucket=copyfroms3 key=parquet/types.parquet.lz4 var-name=lz4_types_url

> COPY INTO types FROM '${lz4_types_url}' (FORMAT PARQUET);

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
2
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
SlimJim
Taco

> DELETE FROM types;
> SELECT count(*) FROM types;
0

# Map and Project.

# Test the AWS Source.


# Test glob patterns.
> COPY INTO types FROM 's3://copyfroms3/parquet' (FORMAT PARQUET, AWS CONNECTION = aws_conn);

> SELECT COUNT(*) FROM types;
18

> SELECT int8_col FROM types ORDER BY int8_col desc;
3
3
3
3
3
3
2
2
2
2
2
2
-1
-1
-1
-1
-1
-1

> SELECT json_col->'a' FROM types ORDER BY 1 NULLS FIRST;
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
5
5
5
5
5
5

> SELECT (record_col).name FROM types ORDER BY 1;
Burger
Burger
Burger
Burger
Burger
Burger
SlimJim
SlimJim
SlimJim
SlimJim
SlimJim
SlimJim
Taco
Taco
Taco
Taco
Taco
Taco

# Test explicit file lists.
