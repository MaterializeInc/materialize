# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A uuid column must reach the Iceberg table as a string column. The default
# Arrow mapping for uuid is FixedSizeBinary(16), which iceberg-rust turns into
# `fixed[16]` (a blob to readers), so `iceberg_type_overrides` remaps uuid to
# Utf8. Both the column type and the text form of the values are asserted: the
# type alone would still pass if the values arrived as raw bytes rendered as
# text.

> CREATE SECRET uuid_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION uuid_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# Row 2 is uppercase on input so the assertions pin the lowercase hyphenated
# rendering, and row 3 covers the null path, which bypasses the uuid builder
# arm entirely.
> CREATE TABLE uuid_rows (id int, u uuid);

> INSERT INTO uuid_rows VALUES
    (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
    (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A22'),
    (3, NULL);

> CREATE SINK uuid_sink
    FROM uuid_rows
    INTO ICEBERG CATALOG CONNECTION uuid_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'uuid_table'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'uuid_sink'
running

$ duckdb-execute name=uuid_iceberg
CREATE SECRET s3_secret_uuid (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;
CREATE SECRET polaris_secret_uuid (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris_uuid (TYPE iceberg, SECRET polaris_secret_uuid, ACCESS_DELEGATION_MODE 'none');

# The column type as the catalog reports it. This is BLOB when uuid falls
# through to the default FixedSizeBinary(16) mapping.
$ duckdb-query name=uuid_iceberg
SELECT typeof(u) FROM polaris_uuid.default_namespace.uuid_table WHERE id = 1
VARCHAR

$ duckdb-query name=uuid_iceberg
SELECT id, u FROM polaris_uuid.default_namespace.uuid_table ORDER BY id
1 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
2 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22
3 <null>

# Reading the data files directly distinguishes a schema-only regression from a
# data-path one.
$ duckdb-query name=uuid_iceberg
SELECT id, u FROM iceberg_scan('s3://test-bucket/default_namespace/uuid_table') ORDER BY id
1 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
2 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22
3 <null>

# The upsert path writes through the same builder as the initial snapshot, and
# an update also exercises the equality-delete writer for a string column.
> UPDATE uuid_rows SET u = 'ffffffff-ffff-ffff-ffff-ffffffffffff' WHERE id = 1;

> UPDATE uuid_rows SET u = NULL WHERE id = 2;

> UPDATE uuid_rows SET u = '11111111-2222-3333-4444-555555555555' WHERE id = 3;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=uuid_iceberg
SELECT id, u FROM polaris_uuid.default_namespace.uuid_table ORDER BY id
1 ffffffff-ffff-ffff-ffff-ffffffffffff
2 <null>
3 11111111-2222-3333-4444-555555555555

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'uuid_sink'
running

> DROP SINK uuid_sink
