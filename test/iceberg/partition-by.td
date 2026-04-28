# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test PARTITION BY syntax for Iceberg sinks.

> CREATE SECRET IF NOT EXISTS access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION IF NOT EXISTS aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION IF NOT EXISTS polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# ---- Positive: identity partition ----

> CREATE TABLE part_identity (region text, v int);

> CREATE SINK part_identity_sink
    FROM part_identity
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_identity_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (region)
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> DROP SINK part_identity_sink;
> DROP TABLE part_identity;

# ---- Positive: temporal partition ----

> CREATE TABLE part_temporal (ts timestamp, v int);

> CREATE SINK part_temporal_sink
    FROM part_temporal
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_temporal_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> DROP SINK part_temporal_sink;
> DROP TABLE part_temporal;

# ---- Positive: bucket partition ----

> CREATE TABLE part_bucket (user_id int, v text);

> CREATE SINK part_bucket_sink
    FROM part_bucket
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_bucket_table'
    )
    USING AWS CONNECTION aws_conn
    KEY (user_id) NOT ENFORCED
    PARTITION BY (bucket(16, user_id))
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

> DROP SINK part_bucket_sink;
> DROP TABLE part_bucket;

# ---- Positive: multiple partition fields ----

> CREATE TABLE part_multi (region text, ts timestamp, v int);

> CREATE SINK part_multi_sink
    FROM part_multi
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_multi_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (region, month(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> DROP SINK part_multi_sink;
> DROP TABLE part_multi;

# ---- Negative: nonexistent column ----

> CREATE TABLE part_bad_col (a int, b text);

! CREATE SINK part_bad_col_sink
    FROM part_bad_col
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_bad_col_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (no_such_col)
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');
contains:column referenced in PARTITION BY does not exist

> DROP TABLE part_bad_col;

# ---- Negative: type incompatibility (day on text) ----

> CREATE TABLE part_bad_type (name text, v int);

! CREATE SINK part_bad_type_sink
    FROM part_bad_type
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_bad_type_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(name))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');
contains:PARTITION BY transform

> DROP TABLE part_bad_type;

# ---- Negative: partition field name collides with schema column ----
# day(foo) generates partition field name "foo_day", which collides
# with the schema column "foo_day".

> CREATE TABLE part_collision (foo date, foo_day text);

! CREATE SINK part_collision_sink
    FROM part_collision
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'part_collision_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(foo))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');
contains:conflicts with schema field

> DROP TABLE part_collision;

# ==========================================================================
# Existing-table validation (table_matches_partitions)
#
# These tests create a sink to materialize the Iceberg table, drop the sink,
# then create a second sink against the same table with a different partition
# spec. The mismatch is detected at runtime by the storage worker.
# ==========================================================================

# ---- Positive: matching partition spec on existing table ----

> CREATE TABLE match_ok (ts timestamp, v int);

> CREATE SINK match_ok_sink_1
    FROM match_ok
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'match_ok_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'match_ok_sink_1'
running

> DROP SINK match_ok_sink_1;

# Same partition spec against the same table — should succeed.
> CREATE SINK match_ok_sink_2
    FROM match_ok
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'match_ok_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'match_ok_sink_2'
running

> DROP SINK match_ok_sink_2;
> DROP TABLE match_ok;

# ---- Negative: partitioned table, no PARTITION BY ----

> CREATE TABLE mismatch_partitioned (region text, v int);

> CREATE SINK mismatch_partitioned_sink_1
    FROM mismatch_partitioned
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_partitioned_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (region)
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'mismatch_partitioned_sink_1'
running

> DROP SINK mismatch_partitioned_sink_1;

# No PARTITION BY against a partitioned table.
> CREATE SINK mismatch_partitioned_sink_2
    FROM mismatch_partitioned
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_partitioned_table'
    )
    USING AWS CONNECTION aws_conn
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT st.error LIKE '%partitioned but sink has no PARTITION BY%'
  FROM mz_sinks s JOIN mz_internal.mz_sink_statuses st ON s.id = st.id
  WHERE s.name = 'mismatch_partitioned_sink_2'
true

> DROP SINK mismatch_partitioned_sink_2;
> DROP TABLE mismatch_partitioned;

# ---- Negative: unpartitioned table, with PARTITION BY ----

> CREATE TABLE mismatch_unpart (region text, v int);

> CREATE SINK mismatch_unpart_sink_1
    FROM mismatch_unpart
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_unpart_table'
    )
    USING AWS CONNECTION aws_conn
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'mismatch_unpart_sink_1'
running

> DROP SINK mismatch_unpart_sink_1;

# PARTITION BY against an unpartitioned table.
> CREATE SINK mismatch_unpart_sink_2
    FROM mismatch_unpart
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_unpart_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (region)
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT st.error LIKE '%unpartitioned but sink specifies PARTITION BY%'
  FROM mz_sinks s JOIN mz_internal.mz_sink_statuses st ON s.id = st.id
  WHERE s.name = 'mismatch_unpart_sink_2'
true

> DROP SINK mismatch_unpart_sink_2;
> DROP TABLE mismatch_unpart;

# ---- Negative: different transforms ----

> CREATE TABLE mismatch_transform (ts timestamp, v int);

> CREATE SINK mismatch_transform_sink_1
    FROM mismatch_transform
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_transform_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (day(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'mismatch_transform_sink_1'
running

> DROP SINK mismatch_transform_sink_1;

# month(ts) against a table partitioned by day(ts).
> CREATE SINK mismatch_transform_sink_2
    FROM mismatch_transform
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'mismatch_transform_table'
    )
    USING AWS CONNECTION aws_conn
    PARTITION BY (month(ts))
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT st.error LIKE '%partition spec mismatch%'
  FROM mz_sinks s JOIN mz_internal.mz_sink_statuses st ON s.id = st.id
  WHERE s.name = 'mismatch_transform_sink_2'
true

> DROP SINK mismatch_transform_sink_2;
> DROP TABLE mismatch_transform;
