# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Sink the contents of the mz_catalog.mz_audit_events system relation to an
# Iceberg table using MODE APPEND.
#
# A sink cannot read a catalog object directly, so we go through a user-created
# materialized view layered over mz_audit_events. The audit log is append-only,
# so MODE APPEND is a natural fit: every emitted row has _mz_diff = 1 and there
# are no _mz_diff = -1 rows.

> CREATE SECRET IF NOT EXISTS audit_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION audit_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# Perform DDL against a uniquely-named object so the audit events we sink are
# deterministic and isolated from the rest of the audit log.
> CREATE TABLE audit_marker_table (a int);

# Pass the entire audit log through unchanged. A view over system objects cannot
# use a wildcard, so every column is named explicitly. The non-deterministic
# columns (id, occurred_at, the id inside details) are filtered out at
# verification time in the DuckDB queries below.
> CREATE MATERIALIZED VIEW mz_audit_events_mv AS
  SELECT id, event_type, object_type, details, "user", occurred_at
  FROM mz_catalog.mz_audit_events;

# MODE APPEND does not take (or permit) a KEY.
> CREATE SINK mz_audit_events_sink
    FROM mz_audit_events_mv
    INTO ICEBERG CATALOG CONNECTION audit_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mz_audit_events_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously; wait for at least one commit interval
# to elapse before querying the table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=audit_iceberg
CREATE SECRET s3_secret_audit (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# The 'create' audit event for our table is written as a single _mz_diff=1 row.
# Filter to our marker table here, since the sink carries the whole audit log.
# `details` is a JSON string, so a plain LIKE on its text isolates our object.
$ duckdb-query name=audit_iceberg
SELECT event_type, object_type, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/mz_audit_events_table') WHERE object_type = 'table' AND details LIKE '%"item":"audit_marker_table"%' ORDER BY _mz_timestamp, event_type
create table 1

# The audit log is append-only: dropping the table emits a new 'drop' event
# (another _mz_diff=1 row) rather than retracting the 'create' event.
> DROP TABLE audit_marker_table;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=audit_iceberg
SELECT event_type, object_type, _mz_diff FROM iceberg_scan('s3://test-bucket/default_namespace/mz_audit_events_table') WHERE object_type = 'table' AND details LIKE '%"item":"audit_marker_table"%' ORDER BY _mz_timestamp, event_type
create table 1
drop table 1

# No deletions are ever written, since the source is append-only.
$ duckdb-query name=audit_iceberg
SELECT count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/mz_audit_events_table') WHERE _mz_diff = -1
0
