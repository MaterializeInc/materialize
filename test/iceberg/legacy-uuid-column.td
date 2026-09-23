# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A sink pointed at a table whose uuid column predates the uuid-as-string
# mapping must keep writing into it. The table holds `fixed[16]` where the sink
# would now create `string`, and a strict schema comparison would halt the sink
# forever. The workflow creates that table through the catalog REST API before
# this file runs.

> CREATE SECRET legacy_uuid_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION legacy_uuid_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE legacy_uuid_rows (id int, u uuid);

> INSERT INTO legacy_uuid_rows VALUES
    (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
    (2, NULL);

> CREATE SINK legacy_uuid_sink
    FROM legacy_uuid_rows
    INTO ICEBERG CATALOG CONNECTION legacy_uuid_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'legacy_uuid_table'
    )
    KEY (id) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

# Before the schema comparison was relaxed this reported `stalled` with
# "schema does not match expected schema", and stayed there across restarts.
> SELECT status, error FROM mz_internal.mz_sink_statuses WHERE name = 'legacy_uuid_sink'
running <null>

$ duckdb-execute name=legacy_uuid_iceberg
CREATE SECRET s3_secret_legacy_uuid (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;
CREATE SECRET polaris_secret_legacy_uuid (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris_legacy_uuid (TYPE iceberg, SECRET polaris_secret_legacy_uuid, ACCESS_DELEGATION_MODE 'none');

# The column keeps its original type. The sink writes into the table as it
# found it rather than rewriting the schema, which is what makes the existing
# binary builder the right one for these rows.
$ duckdb-query name=legacy_uuid_iceberg
SELECT typeof(u) FROM polaris_legacy_uuid.default_namespace.legacy_uuid_table WHERE id = 1
BLOB

# The 16 bytes of the uuid, hex-encoded, confirming the values themselves
# survive the legacy encoding.
$ duckdb-query name=legacy_uuid_iceberg
SELECT id, hex(u) FROM polaris_legacy_uuid.default_namespace.legacy_uuid_table ORDER BY id
1 A0EEBC999C0B4EF8BB6D6BB9BD380A11
2 <null>

# Updates and inserts keep flowing after the table has been accepted.
> UPDATE legacy_uuid_rows SET u = 'ffffffff-ffff-ffff-ffff-ffffffffffff' WHERE id = 1;

> INSERT INTO legacy_uuid_rows VALUES (3, '11111111-2222-3333-4444-555555555555');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-query name=legacy_uuid_iceberg
SELECT id, hex(u) FROM polaris_legacy_uuid.default_namespace.legacy_uuid_table ORDER BY id
1 FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
2 <null>
3 11111111222233334444555555555555

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'legacy_uuid_sink'
running

> DROP SINK legacy_uuid_sink
