# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# An Iceberg sink whose input relation has more columns than the Iceberg table it
# writes to must not panic the storage worker.
#
# `load_or_create_table` hands back a pre-existing Iceberg table without checking
# that its schema still matches the sink's input, and the writer derives its Arrow
# column builders from that table schema (`merge_materialize_metadata_into_iceberg_schema`).
# Once the Materialize relation has gained a column, every row carries more datums
# than the builder has columns, and `ArrowBuilder::add_row` reaches
# `zip_eq` with iterators of different lengths:
#
#   itertools: .zip_eq() reached end of one iterator before the other
#     mz_arrow_util::builder::ArrowBuilder::add_row
#     mz_storage::sink::iceberg::AppendEnvelopeHandler::row_to_batch
#     mz_storage::sink::iceberg::write_data_files

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_alter_table_add_column = true

> CREATE SECRET IF NOT EXISTS evolve_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION evolve_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE evolve_src (a int);

> INSERT INTO evolve_src VALUES (1);

# Creates the Iceberg table with columns (a, _mz_diff, _mz_timestamp).
> CREATE SINK evolve_sink_v1
    FROM evolve_src
    INTO ICEBERG CATALOG CONNECTION evolve_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'evolve_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit asynchronously; wait for the table to be created and the
# first batch committed before dropping the sink.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=evolve_iceberg
CREATE SECRET s3_secret_evolve (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

$ duckdb-query name=evolve_iceberg
SELECT a, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/evolve_table') ORDER BY a
1 1

> DROP SINK evolve_sink_v1;

> ALTER TABLE evolve_src ADD COLUMN b text;

# The new sink's input has two columns, the Iceberg table still has one.
> CREATE SINK evolve_sink_v2
    FROM evolve_src
    INTO ICEBERG CATALOG CONNECTION evolve_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'evolve_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> INSERT INTO evolve_src VALUES (2, 'two');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=10s

# The sink must not have taken the process down with it. Any outcome that keeps
# the storage worker alive (an error status, or a schema evolution that lets the
# write through) is acceptable here, so only assert that we can still talk to the
# cluster hosting the sink.
> SELECT count(*) FROM evolve_src;
2

> SELECT count(*) > 0
    FROM mz_internal.mz_sink_status_history h
    JOIN mz_sinks s ON s.id = h.sink_id
    WHERE s.name = 'evolve_sink_v2'
        AND h.error LIKE '%schema does not match expected schema%';
true
