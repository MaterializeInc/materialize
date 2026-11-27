# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Iceberg sinks write jsonb as a string column of JSON text. A jsonb string
# scalar must keep its quotes and escapes there, else readers see invalid JSON
# (rows 1 and 2) or a value of another JSON type (row 3).

> CREATE SECRET jsonb_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION jsonb_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE jsonb_rows (id int, j jsonb);

> INSERT INTO jsonb_rows VALUES
    (1, '"hello"'),
    (2, '"say \"hi\""'),
    (3, '"123"'),
    (4, '{"k": "v"}');

# The encoding is chosen from the release the sink was created on, which only
# Materialize may record.
! CREATE SINK jsonb_sink_versioned
    FROM jsonb_rows
    INTO ICEBERG CATALOG CONNECTION jsonb_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'jsonb_table_versioned'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s', CREATION VERSION 2600100);
contains:CREATE SINK...WITH (CREATION VERSION..) is not allowed

> CREATE SINK jsonb_sink
    FROM jsonb_rows
    INTO ICEBERG CATALOG CONNECTION jsonb_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'jsonb_table'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

> SELECT create_sql LIKE '%CREATION VERSION%' FROM (SHOW CREATE SINK jsonb_sink)
false

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'jsonb_sink'
running

$ duckdb-execute name=jsonb_iceberg
CREATE SECRET s3_secret_jsonb (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;
CREATE SECRET polaris_secret_jsonb (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris_jsonb (TYPE iceberg, SECRET polaris_secret_jsonb, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM polaris_jsonb.default_namespace.jsonb_table ORDER BY id
1 "hello"
2 "say \"hi\""
3 "123"
4 {"k":"v"}
