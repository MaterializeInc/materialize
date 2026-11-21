# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A jsonb column reaches the Iceberg table as a string column holding JSON
# text. String scalars must keep their quotes and escapes, like the object in
# row 3 does, or readers cannot parse the column as JSON.

> CREATE CONNECTION jsonb_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE jsonb_rows (id int, j jsonb);

> INSERT INTO jsonb_rows VALUES (1, '"hello"'), (2, '"say \"hi\""'), (3, '{"k": "v"}');

> CREATE SINK jsonb_sink
    FROM jsonb_rows
    INTO ICEBERG CATALOG CONNECTION jsonb_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'jsonb_table'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ duckdb-execute name=jsonb_iceberg
CREATE SECRET s3_secret_jsonb (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM iceberg_scan('s3://test-bucket/default_namespace/jsonb_table') ORDER BY id
1 "hello"
2 "say \"hi\""
3 {"k":"v"}
