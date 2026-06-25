# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A fresh Iceberg sink whose input finishes without producing any rows
# is expected to commit an empty snapshot.
# N.B. It would also be correct to make zero commits in this case.
#      The important part is that the sink doesn't break.

> CREATE SECRET empty_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION empty_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# A finite COUNTER generator with AS OF == UP TO produces zero rows and then
# closes its frontier (data at offset >= UP TO is dropped; the generator
# terminates at UP TO). This gives the sink a fresh input that closes having
# seen no data.
> CREATE SOURCE counter_src FROM LOAD GENERATOR COUNTER (AS OF 5, UP TO 5);

> CREATE TABLE counter_tbl FROM SOURCE counter_src;

> SELECT count(*) FROM counter_tbl
0

> CREATE SINK empty_demo
    FROM counter_tbl
    INTO ICEBERG CATALOG CONNECTION empty_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'empty_source_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# The sink must not stall or error.
> SELECT error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'empty_demo'
true

# Iceberg sinks commit asynchronously; wait for at least one commit interval.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

$ duckdb-execute name=empty_iceberg
CREATE SECRET s3_secret_empty (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# We successfully made our one commit. And it is empty.
# A stalled sink would never have committed anything.
$ duckdb-query name=empty_iceberg
SELECT count(*) = 1 FROM iceberg_snapshots('s3://test-bucket/default_namespace/empty_source_table')
true

# The table should hold no data.
$ duckdb-query name=empty_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/empty_source_table')
0
