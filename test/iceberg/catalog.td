# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> create table foo(a int, b text);

> insert into foo values (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK demo
    FROM foo
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'demo_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (a) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously, need to wait for at least one commit interval
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 one
2 two
3 three

$ duckdb-execute name=iceberg
CREATE SECRET polaris_secret (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris (TYPE iceberg, SECRET polaris_secret, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 one
2 two
3 three

# Test updating existing rows
> UPDATE foo SET b = 'ONE' WHERE a = 1;

> UPDATE foo SET b = 'TWO' WHERE a = 2;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 ONE
2 TWO
3 three

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 ONE
2 TWO
3 three

# Test adding new rows
> INSERT INTO foo VALUES (4, 'four'), (5, 'five');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 ONE
2 TWO
3 three
4 four
5 five

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 ONE
2 TWO
3 three
4 four
5 five

# Test deleting rows
> DELETE FROM foo WHERE a = 2;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 ONE
3 three
4 four
5 five

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 ONE
3 three
4 four
5 five

# Test multiple deletes
> DELETE FROM foo WHERE a IN (3, 5);

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 ONE
4 four

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 ONE
4 four

# Test a mix of operations: add, update, and verify
> INSERT INTO foo VALUES (6, 'six'), (7, 'seven');

> UPDATE foo SET b = 'FOUR' WHERE a = 4;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 ONE
4 FOUR
6 six
7 seven

$ duckdb-query name=iceberg
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 ONE
4 FOUR
6 six
7 seven

# Test unsigned integer types (uint8 = UInt64)
# Iceberg doesn't support UInt64 natively, so we convert to Decimal(20,0)
> create table bar(id uint8, name text);

> insert into bar values (1, 'one'), (2, 'two'), (18446744073709551615, 'max');

> CREATE SINK uint_demo
    FROM bar
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'uint_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/uint_table') ORDER BY id
1 one
2 two
18446744073709551615 max

# Test updating uint8 values
> UPDATE bar SET name = 'ONE' WHERE id = 1;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/uint_table') ORDER BY id
1 ONE
2 two
18446744073709551615 max

# Test interval types
# Iceberg doesn't support interval natively, so we store as string.
> CREATE TABLE intervals(id int, dur interval);

> INSERT INTO intervals VALUES (1, interval '1 day'), (2, interval '2 hours 30 minutes'), (3, interval '1 year 3 months'), (4, interval '0 seconds');

> CREATE SINK interval_demo
    FROM intervals
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'interval_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=iceberg
SELECT id, dur FROM iceberg_scan('s3://test-bucket/default_namespace/interval_table') ORDER BY id
1 1 day
2 02:30:00
3 1 year 3 months
4 00:00:00

# Test updating interval values
> UPDATE intervals SET dur = interval '5 days 12 hours' WHERE id = 1;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT id, dur FROM iceberg_scan('s3://test-bucket/default_namespace/interval_table') ORDER BY id
1 5 days 12:00:00
2 02:30:00
3 1 year 3 months
4 00:00:00

# Test range types (int4range)
# Ranges are stored as an Iceberg struct with fields: lower, upper, lower_inclusive, upper_inclusive, empty.
# Materialize canonicalizes integer ranges to [inclusive, exclusive) form.
> CREATE TABLE ranges(id int, r int4range);

> INSERT INTO ranges VALUES (1, '[1,10]'::int4range), (2, '(5,15)'::int4range), (3, 'empty'::int4range), (4, '[,100)'::int4range);

> CREATE SINK range_demo
    FROM ranges
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'range_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

# Canonical forms: [1,10] -> [1,11), (5,15) -> [6,15), empty -> empty, [,100) -> (null,100)
$ duckdb-query name=iceberg
SELECT id, r.lower, r.upper, r.lower_inclusive, r.upper_inclusive, r.empty FROM iceberg_scan('s3://test-bucket/default_namespace/range_table') ORDER BY id
1 1 11 true false false
2 6 15 true false false
3 <null> <null> false false true
4 <null> 100 false false false

# Test updating range values
> UPDATE ranges SET r = '[20,30]'::int4range WHERE id = 1;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=iceberg
SELECT id, r.lower, r.upper, r.lower_inclusive, r.upper_inclusive, r.empty FROM iceberg_scan('s3://test-bucket/default_namespace/range_table') ORDER BY id
1 20 31 true false false
2 6 15 true false false
3 <null> <null> false false true
4 <null> 100 false false false
