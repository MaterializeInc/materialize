# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO jsonb_rows VALUES (1, '"hello"'), (2, '"say \"hi\""'), (3, '{"k": "v"}');

$ duckdb-execute name=jsonb_iceberg
CREATE SECRET s3_secret_jsonb (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;
CREATE SECRET jsonb_catalog_secret (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS jsonb_catalog (TYPE iceberg, SECRET jsonb_catalog_secret, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM iceberg_scan('s3://test-bucket/default_namespace/jsonb_table') ORDER BY id
1 "hello"
2 "say \"hi\""
3 {"k":"v"}

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM jsonb_catalog.default_namespace.legacy_jsonb ORDER BY id
1 hello
2 say "hi"
3 {"k":"v"}

# Rehydrate both sinks after they have committed data in their chosen encoding.
> ALTER SINK jsonb_sink SET (COMMIT INTERVAL '2s');
> ALTER SINK legacy_jsonb_sink SET (COMMIT INTERVAL '2s');

> UPDATE jsonb_rows SET id = 4 WHERE id = 1;
> DELETE FROM jsonb_rows WHERE id = 2;

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM jsonb_catalog.default_namespace.legacy_jsonb ORDER BY id
3 {"k":"v"}
4 hello

$ duckdb-query name=jsonb_iceberg
SELECT id, j FROM jsonb_catalog.default_namespace.jsonb_table ORDER BY id
3 {"k":"v"}
4 "hello"
