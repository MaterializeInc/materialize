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
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously, need to wait for at least one commit interval
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=iceberg sort-rows=true
SELECT a, b FROM iceberg_scan('s3://test-bucket/default_namespace/demo_table') ORDER BY a
1 one
2 two
3 three

$ duckdb-execute name=iceberg
CREATE SECRET polaris_secret (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris (TYPE iceberg, SECRET polaris_secret, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=iceberg sort-rows=true
SELECT a, b FROM polaris.default_namespace.demo_table ORDER BY a
1 one
2 two
3 three
