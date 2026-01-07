# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CONNECTION s3tables_e2e TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE = '${arg.warehouse-arn}',
    AWS CONNECTION = aws_assume_role_s3tablesrest_e2e
  );

> create table foo(a int, b text);

> insert into foo values (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK demo
    FROM foo
    INTO ICEBERG CATALOG CONNECTION s3tables_e2e (
        NAMESPACE 'default_namespace',
        TABLE 'demo_table'
    )
    USING AWS CONNECTION aws_assume_role_s3tablesrest_e2e
    KEY (a) NOT ENFORCED
    ENVELOPE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously, need to wait for at least one commit interval
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=20s

$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID '${arg.aws-access-key-id}', SECRET '${arg.aws-secret-access-key}', SESSION_TOKEN '${arg.aws-session-token}');
ATTACH '${arg.warehouse-arn}' AS s3tables_e2e (TYPE iceberg, ENDPOINT_TYPE s3_tables);

$ duckdb-query name=iceberg
SELECT a, b FROM s3tables_e2e.default_namespace.demo_table ORDER BY a
1 one
2 two
3 three

> DROP SINK demo;
