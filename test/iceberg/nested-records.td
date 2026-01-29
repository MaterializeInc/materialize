# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test nested record data types in Iceberg sinks

> CREATE SECRET nested_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION nested_aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET nested_access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION nested_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# Test simple record type
> CREATE TYPE address AS (street text, city text, zip int);

> CREATE TABLE users_with_address (
    id int,
    name text,
    addr address
  );

> INSERT INTO users_with_address VALUES
    (1, 'Alice', ROW('123 Main St', 'Springfield', 12345)),
    (2, 'Bob', ROW('456 Oak Ave', 'Shelbyville', 67890)),
    (3, 'Charlie', ROW('789 Pine Rd', 'Capital City', 11111));

> CREATE SINK nested_record_sink
    FROM users_with_address
    INTO ICEBERG CATALOG CONNECTION nested_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'nested_record_table'
    )
    USING AWS CONNECTION nested_aws_conn
    KEY (id) NOT ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-execute name=nested_iceberg
CREATE SECRET s3_secret_nested (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=nested_iceberg
SELECT id, name, addr.street, addr.city, addr.zip FROM iceberg_scan('s3://test-bucket/default_namespace/nested_record_table') ORDER BY id
1 Alice 123 Main St Springfield 12345
2 Bob 456 Oak Ave Shelbyville 67890
3 Charlie 789 Pine Rd Capital City 11111

# Test updating records with nested data
> UPDATE users_with_address SET addr = ROW('999 New St', 'New Town', 99999) WHERE id = 1;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=nested_iceberg
SELECT id, name, addr.street, addr.city, addr.zip FROM iceberg_scan('s3://test-bucket/default_namespace/nested_record_table') ORDER BY id
1 Alice 999 New St New Town 99999
2 Bob 456 Oak Ave Shelbyville 67890
3 Charlie 789 Pine Rd Capital City 11111

# Test deeply nested records (record containing record)
> CREATE TYPE inner_record AS (x int, y text);

> CREATE TYPE outer_record AS (inner inner_record, z bool);

> CREATE TABLE deeply_nested (
    id int,
    data outer_record
  );

> INSERT INTO deeply_nested VALUES
    (1, ROW(ROW(10, 'ten'), true)),
    (2, ROW(ROW(20, 'twenty'), false)),
    (3, ROW(ROW(30, 'thirty'), true));

> CREATE SINK deeply_nested_sink
    FROM deeply_nested
    INTO ICEBERG CATALOG CONNECTION nested_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'deeply_nested_table'
    )
    USING AWS CONNECTION nested_aws_conn
    KEY (id) NOT ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=nested_iceberg
SELECT id, data.inner.x, data.inner.y, data.z FROM iceberg_scan('s3://test-bucket/default_namespace/deeply_nested_table') ORDER BY id
1 10 ten true
2 20 twenty false
3 30 thirty true

# Test record with nullable fields
> CREATE TYPE nullable_record AS (required_field int, optional_field text);

> CREATE TABLE with_nullable_record (
    id int,
    rec nullable_record
  );

> INSERT INTO with_nullable_record VALUES
    (1, ROW(100, 'has value')),
    (2, ROW(200, NULL)),
    (3, NULL);

> CREATE SINK nullable_record_sink
    FROM with_nullable_record
    INTO ICEBERG CATALOG CONNECTION nested_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'nullable_record_table'
    )
    USING AWS CONNECTION nested_aws_conn
    KEY (id) NOT ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=nested_iceberg
SELECT id, rec.required_field, rec.optional_field FROM iceberg_scan('s3://test-bucket/default_namespace/nullable_record_table') ORDER BY id
1 100 has value
2 200 <null>
3 <null> <null>
