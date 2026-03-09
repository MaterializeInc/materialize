# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for COPY TO expr.

# COPY TO expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

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

! COPY t TO 's3://path/to/dir' WITH (FORMAT = 'csv');
contains:AWS CONNECTION is required for COPY ... TO <expr>

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'text'
  );
contains:FORMAT TEXT not yet supported

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'binary'
  );
contains:FORMAT BINARY not yet supported

! COPY t TO 's3://path/to/dir'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = '20MB'
  );
contains:COPY TO <expr> requires a FORMAT option

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
> CREATE CLUSTER c1 REPLICAS (r1 (size 'scale=1,workers=2'), r2 (size 'scale=1,workers=2'));
> SET cluster = c1;

$ set-from-sql var=key-1
SELECT TO_CHAR(now(), 'YYYY-MM-DD')

# functions like now() should work in the s3 path
> COPY t TO 's3://copytos3/test/1/' || TO_CHAR(now(), 'YYYY-MM-DD')
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> CREATE TABLE t_from_1 (a INT)
> COPY INTO t_from_1 FROM 's3://copytos3/test/1/${key-1}' (FORMAT CSV, AWS CONNECTION = aws_conn)
> SELECT a FROM t_from_1 ORDER BY 1
1
2

$ set-from-sql var=key-2
SELECT TO_CHAR(now(), 'YYYY-MM-DD')

# The test depends on the day not changing at this specific time
> SELECT '${key-1}' = '${key-2}'
true

> SELECT a FROM t
1
2


> COPY (SELECT a FROM t) TO 's3://copytos3/test/2'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> CREATE TABLE t_from_2 (a INT)
> COPY INTO t_from_2 FROM 's3://copytos3/test/2' (FORMAT CSV, AWS CONNECTION = aws_conn)
> SELECT a FROM t_from_2 ORDER BY 1
1
2

# copy should work on non-table objects (views, etc)
> CREATE VIEW my_view AS SELECT a FROM t WHERE a < 2;

> COPY my_view TO 's3://copytos3/test/2_5'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv',
    HEADER = true
  );

> CREATE TABLE t1 (a INT);

> COPY INTO t1 FROM 's3://copytos3/test/2_5' (
    FORMAT CSV,
    AWS CONNECTION = aws_conn,
    HEADER = true
  );

> SELECT * FROM t1;
1

> COPY (SELECT array[1,2]::int[], false::bool, 'Inf'::double, '{"s": "abc"}'::jsonb, 1::mz_timestamp, 32767::smallint, 2147483647::integer, 9223372036854775807::bigint, 12345678901234567890123.4567890123456789::numeric(39,16), '2010-10-10'::date, '10:10:10'::time, '2010-10-10 10:10:10+00'::timestamp, '2010-10-10 10:10:10+02'::timestamptz, '0 day'::interval, 'aaaa'::text, '\\xAAAA'::bytea, 'това е'::text, 'текст'::bytea) TO 's3://copytos3/test/3'
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

> CREATE TABLE t_from_4 (n INT)
> COPY INTO t_from_4 FROM 's3://copytos3/test/4' (FORMAT CSV, AWS CONNECTION = aws_conn)
> SELECT COUNT(*) FROM t_from_4
1000000

# test CSV format options
> COPY (SELECT array[1,2]::int[], 83647::integer, '{"s": "ab`c"}'::jsonb, '2010-10-10 10:10:10+00'::timestamp) TO 's3://copytos3/test/4_5'
  WITH (
    AWS CONNECTION = aws_conn,
    FORMAT = 'csv',
    DELIMITER = ';',
    QUOTE = '`',
    HEADER = true
  )

> CREATE TABLE t_from_4_5 (c1 INT[], c2 INT, c3 JSONB, c4 TIMESTAMP)
> COPY INTO t_from_4_5 FROM 's3://copytos3/test/4_5' (
    FORMAT CSV,
    AWS CONNECTION = aws_conn,
    DELIMITER = ';',
    QUOTE = '`',
    HEADER = true
  )
> SELECT c1, c2, c3, c4 FROM t_from_4_5
{1,2} 83647 "{\"s\":\"ab`c\"}" "2010-10-10 10:10:10"

> COPY (SELECT 1 WHERE FALSE) TO 's3://copytos3/test/5'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> CREATE TABLE t_from_5 (a INT)
> COPY INTO t_from_5 FROM 's3://copytos3/test/5' (FORMAT CSV, AWS CONNECTION = aws_conn)
> SELECT COUNT(*) FROM t_from_5
0

$ s3-verify-data bucket=copytos3 key=test/1/${key-1} sort-rows=true
1
2

$ s3-verify-data bucket=copytos3 key=test/2 sort-rows=true
1
2

# The double `a` here is a result of the header being written once per file.
$ s3-verify-data bucket=copytos3 key=test/2_5 sort-rows=true
a
a
1

$ s3-verify-data bucket=copytos3 key=test/3 sort-rows=true
"{1,2}",f,Infinity,"{""s"":""abc""}",1,32767,2147483647,9223372036854775807,12345678901234567890123.4567890123456789,2010-10-10,10:10:10,2010-10-10 10:10:10,2010-10-10 08:10:10+00,00:00:00,aaaa,\x5c7841414141,това е,\xd182d0b5d0bad181d182

$ s3-verify-data bucket=copytos3 key=test/4_5 sort-rows=true
array;int4;jsonb;timestamp
{1,2};83647;`{"s":"ab``c"}`;2010-10-10 10:10:10

# Ensure that at least one file is written even when the input is empty.
$ s3-verify-keys bucket=copytos3 prefix-path=test/5 key-pattern=^test/5/mz.*\.csv$

# Copy a large amount of data in the background and check to see that the INCOMPLETE
# sentinel object is written during the copy

# TODO(database-issues#7984): Enable this test once it is more reliable
# $ postgres-execute background=true connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
# COPY (SELECT * FROM generate_series(1, 50000000)) TO 's3://copytos3/test/5' WITH (AWS CONNECTION = aws_conn, MAX FILE SIZE = "100MB", FORMAT = 'csv');

# $ s3-verify-keys bucket=copytos3 prefix-path=test/5 key-pattern=INCOMPLETE


# Test with parquet formatting

$ set-from-sql var=key-1
SELECT TO_CHAR(now(), 'YYYY-MM-DD')

> COPY t TO 's3://copytos3/parquet_test/1/' || TO_CHAR(now(), 'YYYY-MM-DD')
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );

$ set-from-sql var=key-2
SELECT TO_CHAR(now(), 'YYYY-MM-DD')

# The test depends on the day not changing at this specific time
> SELECT '${key-1}' = '${key-2}'
true

> COPY (SELECT a FROM t) TO 's3://copytos3/parquet_test/2'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );

> COPY (SELECT array[1,2]::int[], array[array[1, 2], array[NULL, 4]], false::bool, 'Inf'::double, '{"s": "abc"}'::jsonb, '85907cb9-ac9b-4e35-84b8-60dc69368aca'::uuid, 1::mz_timestamp, 32767::smallint, 2147483647::integer, 9223372036854775807::bigint, 1234567890123456789012.4567890123456789::numeric(38,16), '2010-10-10'::date, '10:10:10'::time, '2010-10-10 10:10:10+00'::timestamp, '2010-10-10 10:10:10+02'::timestamptz, 'aaaa'::text, '\\xAAAA'::bytea, 'това е'::text, 'текст'::bytea) TO 's3://copytos3/parquet_test/3'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );

> COPY (SELECT 1 WHERE FALSE) TO 's3://copytos3/parquet_test/4'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );

$ s3-verify-data bucket=copytos3 key=parquet_test/1/${key-1} sort-rows=true
1
2

$ s3-verify-data bucket=copytos3 key=parquet_test/2 sort-rows=true
1
2

$ s3-verify-data bucket=copytos3 key=parquet_test/3 sort-rows=true
{items: [1, 2], dimensions: 1} {items: [1, 2, , 4], dimensions: 2} false inf {"s":"abc"} 85907cb9ac9b4e3584b860dc69368aca 1 32767 2147483647 9223372036854775807 1234567890123456789012.4567890123456789 2010-10-10 10:10:10 2010-10-10T10:10:10 2010-10-10T08:10:10Z aaaa 5c7841414141 това е d182d0b5d0bad181d182

# Ensure that at least one file is written even when the input is empty.
$ s3-verify-keys bucket=copytos3 prefix-path=parquet_test/4 key-pattern=^parquet_test/4/mz.*\.parquet$

# Confirm that unimplemented types will early exit before writing to s3
! COPY (SELECT '0 day'::interval)  TO 's3://copytos3/parquet_test/5'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
contains:Cannot encode the following columns/types: ["interval: Interval"]

# now should succeed since incomplete sentinel was never written
> COPY (SELECT 1::int)  TO 's3://copytos3/parquet_test/5'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );

$ s3-verify-data bucket=copytos3 key=parquet_test/5 sort-rows=true
1

# Tests for decimal / numeric type

! COPY (SELECT .123456789012345678901234567890123456789::numeric(39,39)) TO 's3://copytos3/parquet_test/6'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');
contains:Cannot encode the following columns/types: ["numeric: Numeric { max_scale: Some(NumericMaxScale(39)) }"]

! COPY (SELECT 12345678901234567890123.4567890123456789::numeric(38,16)) TO 's3://copytos3/parquet_test/6'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');
contains:out of range for column with precision

! COPY (SELECT 'NaN'::numeric(10,5)) TO 's3://copytos3/parquet_test/7'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');
contains:Cannot represent special numeric value

# the default column scale will be set to 10 so if we see a value with more than 10 digits after the
# decimal it cannot be represented in this column
! COPY (SELECT 5.4567890123456789::numeric) TO 's3://copytos3/parquet_test/8'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');
contains:cannot represent decimal value

> CREATE TABLE t2 (a NUMERIC, b NUMERIC(38, 15), c NUMERIC(38, 0), d NUMERIC(38, 5));
> INSERT INTO t2 VALUES (1234, -1234, 1234, 1234);
> INSERT INTO t2 VALUES (123456789.123456789, -123456789.12345678912345, 1234567890000, 123456789.12345);
> INSERT INTO t2 VALUES (10000000000000000001, 100000000000000.0000001, -100000000, 100000000000000000000000000000000.00001)

> SELECT a, b, c, d FROM t2
1234 -1234 1234 1234
123456789.123456789 -123456789.12345678912345 1234567890000 123456789.12345
10000000000000000001 100000000000000.0000001 -100000000 100000000000000000000000000000000.00001

> COPY t2 TO 's3://copytos3/parquet_test/9'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');

# each column will use scale 10 or the defined max-scale of the input column, so many values will
# be padded with 0s after the decimal point to fit the column scale but are still the same number
$ s3-verify-data bucket=copytos3 key=parquet_test/9 sort-rows=true
1234.0000000000 -1234.000000000000000 1234 1234.00000
123456789.1234567890 -123456789.123456789123450 1234567890000 123456789.12345
10000000000000000001.0000000000 100000000000000.000000100000000 -100000000 100000000000000000000000000000000.00001

# Tests for nested types

> CREATE TABLE t3 (c1 real list list, c2 varchar list, c3 MAP[text=>MAP[text=>double]], c4 int[][]);
> INSERT INTO t3 VALUES (LIST[[1.25, 2.5],[223.3333]], LIST['a', 'b'], '{a=>{b=>2.5}}', ARRAY[[1, 2],[3, 5]]);
> INSERT INTO t3 VALUES (LIST[[0.0], NULL], NULL, NULL, NULL);
> INSERT INTO t3 VALUES (NULL, LIST[NULL], '{a=>NULL}', ARRAY[[1, 2], [NULL, NULL]]::int[][]);

> COPY t3 TO 's3://copytos3/parquet_test/10' WITH (AWS CONNECTION = aws_conn, FORMAT = 'parquet');

$ s3-verify-data bucket=copytos3 key=parquet_test/10 sort-rows=true
[[0.0], ]   // allow-trailing-whitespace
 [] {a: } {items: [1, 2, , ], dimensions: 2}
[[1.25, 2.5], [223.3333]] [a, b] {a: {b: 2.5}} {items: [1, 2, 3, 5], dimensions: 2}

# Roundtrip corner case tests: exact data verification across COPY TO / COPY FROM

# Newlines inside strings: embedded LF and CRLF must survive the CSV quoting round-trip
> CREATE TABLE t_newlines (id INT, val TEXT)
> INSERT INTO t_newlines VALUES
  (1, E'first line\nsecond line'),
  (2, E'carriage\r\nreturn'),
  (3, 'no newline here')

> COPY t_newlines TO 's3://copytos3/roundtrip/newlines'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv')

> CREATE TABLE t_newlines_from (id INT, val TEXT)
> COPY INTO t_newlines_from FROM 's3://copytos3/roundtrip/newlines' (FORMAT CSV, AWS CONNECTION = aws_conn)

> SELECT id, replace(replace(val, E'\r\n', '<CRLF>'), E'\n', '<LF>') FROM t_newlines_from ORDER BY id
1 "first line<LF>second line"
2 "carriage<CRLF>return"
3 "no newline here"

# Unicode: CJK, Cyrillic, emoji, Arabic, and Latin extended characters
> CREATE TABLE t_unicode (id INT, val TEXT)
> INSERT INTO t_unicode VALUES
  (1, '日本語テスト'),
  (2, 'Привет мир'),
  (3, '🎉🚀💡'),
  (4, 'مرحبا بالعالم'),
  (5, E'\u00e9\u00e0\u00fc\u00f1')

> COPY t_unicode TO 's3://copytos3/roundtrip/unicode'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv')

> CREATE TABLE t_unicode_from (id INT, val TEXT)
> COPY INTO t_unicode_from FROM 's3://copytos3/roundtrip/unicode' (FORMAT CSV, AWS CONNECTION = aws_conn)

> SELECT id, val FROM t_unicode_from ORDER BY id
1 日本語テスト
2 "Привет мир"
3 🎉🚀💡
4 "مرحبا بالعالم"
5 éàüñ

# Integer and float edge cases: bigint extremes, float specials (Infinity, -Infinity, NaN)
> CREATE TABLE t_numbers (id INT, a BIGINT, b FLOAT8, c FLOAT4)
> INSERT INTO t_numbers VALUES
  (1,  9223372036854775807,  'Infinity'::float8,  'Infinity'::float4),
  (2, -9223372036854775808, '-Infinity'::float8, '-Infinity'::float4),
  (3,  0,                   'NaN'::float8,        'NaN'::float4),
  (4,  1,                    0.1,                  0.1::float4),
  (5, -1,                    1.0/3.0,             (1.0/3.0)::float4)

> COPY t_numbers TO 's3://copytos3/roundtrip/numbers'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv')

> CREATE TABLE t_numbers_from (id INT, a BIGINT, b FLOAT8, c FLOAT4)
> COPY INTO t_numbers_from FROM 's3://copytos3/roundtrip/numbers' (FORMAT CSV, AWS CONNECTION = aws_conn)

# Verify bigint extremes and float Infinity/-Infinity
> SELECT id, a, b, c FROM t_numbers_from WHERE id IN (1, 2) ORDER BY id
1 9223372036854775807 inf inf
2 -9223372036854775808 -inf -inf

# NaN: use text cast since NaN != NaN under IEEE 754
> SELECT id, a, b::text, c::text FROM t_numbers_from WHERE id = 3
3 0 NaN NaN

# Float precision: 0.1 and 1/3 must roundtrip to the same bit pattern
> SELECT COUNT(*) FROM t_numbers_from t1
  JOIN t_numbers t2 USING (id)
  WHERE t1.b = t2.b AND t1.c = t2.c AND id IN (4, 5)
2

# Large numerics: values near the 38-digit precision limit, positive and negative
> CREATE TABLE t_numerics (id INT, a NUMERIC(38,0), b NUMERIC(38,10), c NUMERIC)
> INSERT INTO t_numerics VALUES
  (1,  99999999999999999999999999999999999999,  9999999999999999999999999999.9999999999,  123456789.987654321),
  (2, -99999999999999999999999999999999999999, -9999999999999999999999999999.9999999999, -123456789.987654321),
  (3,  0,                                       0,                                         0)

> COPY t_numerics TO 's3://copytos3/roundtrip/numerics'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv')

> CREATE TABLE t_numerics_from (id INT, a NUMERIC(38,0), b NUMERIC(38,10), c NUMERIC)
> COPY INTO t_numerics_from FROM 's3://copytos3/roundtrip/numerics' (FORMAT CSV, AWS CONNECTION = aws_conn)

> SELECT id, a, b, c FROM t_numerics_from ORDER BY id
1 99999999999999999999999999999999999999 9999999999999999999999999999.9999999999 123456789.987654321
2 -99999999999999999999999999999999999999 -9999999999999999999999999999.9999999999 -123456789.987654321
3 0 0 0

# Many columns: a 30-column table spanning multiple types to stress CSV column alignment
> CREATE TABLE t_wide (
    c01 SMALLINT,      c02 INT,            c03 BIGINT,
    c04 FLOAT4,        c05 FLOAT8,         c06 NUMERIC(10,2),
    c07 BOOL,          c08 TEXT,           c09 DATE,
    c10 TIME,          c11 TIMESTAMP,      c12 SMALLINT,
    c13 INT,           c14 BIGINT,         c15 FLOAT4,
    c16 FLOAT8,        c17 NUMERIC(10,2),  c18 BOOL,
    c19 TEXT,          c20 DATE,           c21 TIME,
    c22 TIMESTAMP,     c23 INT,            c24 INT,
    c25 INT,           c26 INT,            c27 INT,
    c28 INT,           c29 INT,            c30 INT
  )

> INSERT INTO t_wide VALUES (
    32767,              -2147483648,         9223372036854775807,
    3.14::float4,       3.141592653589793,  99999999.99,
    true,              'col08',             '2024-02-29',
    '12:34:56',        '2024-02-29 12:34:56', -32768,
    2147483647,        -9223372036854775808, -3.14::float4,
    -2.718281828459045, -99999999.99,       false,
    'col19',           '1970-01-01',        '00:00:00',
    '1970-01-01 00:00:00', 23,             24,
    25,                 26,                 27,
    28,                 29,                 30
  )

> COPY t_wide TO 's3://copytos3/roundtrip/wide'
  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv')

> CREATE TABLE t_wide_from (
    c01 SMALLINT,      c02 INT,            c03 BIGINT,
    c04 FLOAT4,        c05 FLOAT8,         c06 NUMERIC(10,2),
    c07 BOOL,          c08 TEXT,           c09 DATE,
    c10 TIME,          c11 TIMESTAMP,      c12 SMALLINT,
    c13 INT,           c14 BIGINT,         c15 FLOAT4,
    c16 FLOAT8,        c17 NUMERIC(10,2),  c18 BOOL,
    c19 TEXT,          c20 DATE,           c21 TIME,
    c22 TIMESTAMP,     c23 INT,            c24 INT,
    c25 INT,           c26 INT,            c27 INT,
    c28 INT,           c29 INT,            c30 INT
  )

> COPY INTO t_wide_from FROM 's3://copytos3/roundtrip/wide' (FORMAT CSV, AWS CONNECTION = aws_conn)

> SELECT COUNT(*) FROM t_wide_from
1

# Verify exact round-trip using symmetric EXCEPT: no rows should appear in one table but not the other
> SELECT COUNT(*) FROM (SELECT * FROM t_wide EXCEPT SELECT * FROM t_wide_from) AS diff
0

> SELECT COUNT(*) FROM (SELECT * FROM t_wide_from EXCEPT SELECT * FROM t_wide) AS diff
0
