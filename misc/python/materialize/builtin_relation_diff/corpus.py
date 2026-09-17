# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The user-object corpus applied to both sides in corpus mode."""

# Statements that need the system account, applied before CORPUS: network
# policy creation, index options and a disabled compaction window are
# flag-gated, and workload classes are settable only by system users.
SYSTEM_CORPUS = [
    "ALTER SYSTEM SET enable_network_policies = true",
    "ALTER SYSTEM SET enable_alter_table_add_column = true",
    "ALTER SYSTEM SET enable_index_options = true",
    "ALTER SYSTEM SET enable_unlimited_retain_history = true",
    "ALTER CLUSTER quickstart SET (WORKLOAD CLASS 'corpus_wc')",
]

# User objects covering the edge classes of mz_object_dependencies: relation,
# function and type references, casts, arrays, custom types, secrets,
# connection-to-connection and connection-to-secret references, sources with
# subsources, and temporary items (which must appear on neither side).
#
# Sinks are absent: they need a live Kafka broker, and their references flow
# through the same id-extraction path as sources and connections.
CORPUS = [
    "CREATE TABLE t (a int, b text NOT NULL, c int4[], d map[text => int8 list])",
    # The element type uuid appears nowhere else in the corpus, so the
    # baseline's element-type edge is not shadowed by a direct reference to
    # uuid. This row exercises the is_dropped_element_ref_edge allowance.
    "CREATE TABLE arr_only (x uuid[])",
    "CREATE VIEW v AS SELECT pg_catalog.abs(a) AS a_abs, b::text AS b_txt FROM t WHERE a > 42",
    "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT count(*) AS c FROM v",
    """CREATE MATERIALIZED VIEW mv_wmr IN CLUSTER quickstart AS
        WITH MUTUALLY RECURSIVE c (a int) AS (SELECT a FROM t UNION SELECT a FROM c)
        SELECT * FROM c""",
    "CREATE INDEX t_idx ON t (abs(a))",
    "CREATE SECRET pw AS 'hunter2'",
    "CREATE CONNECTION ssh_conn TO SSH TUNNEL (HOST 'unused', USER 'mz', PORT 22)",
    """CREATE CONNECTION kafka_conn TO KAFKA (
        BROKER 'unused:9092',
        SSH TUNNEL ssh_conn,
        SASL MECHANISMS 'PLAIN',
        SASL USERNAME 'u',
        SASL PASSWORD = SECRET pw
    ) WITH (VALIDATE = false)""",
    "CREATE SOURCE auction IN CLUSTER quickstart FROM LOAD GENERATOR AUCTION FOR ALL TABLES",
    "CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4)",
    "CREATE TYPE int4_list_map AS MAP (KEY TYPE = text, VALUE TYPE = int4_list)",
    # Rows for the cluster, replica, privilege and audit-event relations:
    # managed and unmanaged clusters, a default-privilege grant, and one
    # audit event of each event_type (the statements above cover create).
    "CREATE CLUSTER c_managed (SIZE 'scale=1,workers=1', REPLICATION FACTOR 2)",
    """CREATE CLUSTER c_unmanaged REPLICAS (
        r1 (SIZE 'scale=1,workers=1'),
        r2 (SIZE 'scale=1,workers=2')
    )""",
    "ALTER DEFAULT PRIVILEGES FOR ROLE materialize IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC",
    "GRANT SELECT ON TABLE t TO PUBLIC",
    "REVOKE SELECT ON TABLE t FROM PUBLIC",
    "COMMENT ON TABLE t IS 'corpus comment'",
    "COMMENT ON COLUMN t.a IS 'corpus column comment'",
    # A second version of a table is the only way a catalog item holds more
    # than one `GlobalId`, which is what the extra-versions rows of
    # mz_object_global_ids record.
    "CREATE TABLE versioned (a int)",
    "ALTER TABLE versioned ADD COLUMN b text",
    "CREATE TABLE renamed (a int)",
    "ALTER TABLE renamed RENAME TO renamed2",
    "CREATE TABLE dropped (a int)",
    "DROP TABLE dropped",
    # Rows for the role, network-policy and AWS-connection relations.
    "CREATE ROLE corpus_role",
    "CREATE ROLE corpus_member",
    "GRANT corpus_role TO corpus_member",
    "ALTER ROLE corpus_role SET cluster = 'c_managed'",
    """CREATE NETWORK POLICY corpus_np (RULES (
        r1 (address='12.34.56.0/24', action='allow', direction='ingress')
    ))""",
    """CREATE CONNECTION aws_conn TO AWS (
        ACCESS KEY ID = 'unused',
        SECRET ACCESS KEY = SECRET pw,
        REGION = 'us-east-1'
    ) WITH (VALIDATE = false)""",
    # Rows for mz_history_retention_strategies: every RETAIN HISTORY spelling
    # planning accepts (a string, an interval literal, a bare number of
    # seconds, a disabled window) on each item kind that has a compaction
    # window, plus a window set and one reset after creation. The items
    # created above cover the default window.
    "CREATE TABLE t_retain (a int) WITH (RETAIN HISTORY FOR '1h')",
    "CREATE TABLE t_retain_interval (a int) WITH (RETAIN HISTORY FOR INTERVAL '2' DAY)",
    "CREATE TABLE t_retain_seconds (a int) WITH (RETAIN HISTORY FOR 90)",
    "CREATE TABLE t_retain_disabled (a int) WITH (RETAIN HISTORY FOR '0')",
    """CREATE SOURCE counter_retain IN CLUSTER quickstart
        FROM LOAD GENERATOR COUNTER WITH (RETAIN HISTORY FOR '2h')""",
    "CREATE INDEX v_retain_idx ON v (a_abs) WITH (RETAIN HISTORY FOR '3h')",
    """CREATE MATERIALIZED VIEW mv_retain IN CLUSTER quickstart
        WITH (RETAIN HISTORY FOR '4h') AS SELECT a FROM t""",
    "ALTER TABLE t SET (RETAIN HISTORY FOR '5h')",
    "ALTER INDEX t_idx SET (RETAIN HISTORY FOR '6h')",
    "ALTER INDEX t_idx RESET (RETAIN HISTORY)",
    # Rows for mz_materialized_view_refresh_strategies: REFRESH AT and REFRESH
    # EVERY with literal times, and several options on one view. Times derived
    # from mz_now() (REFRESH AT CREATION, an omitted ALIGNED TO) are absent on
    # purpose: they are wall-clock values that differ between the two
    # environments by construction.
    """CREATE MATERIALIZED VIEW mv_refresh_at IN CLUSTER quickstart
        WITH (REFRESH AT '2999-01-01 00:00:00+00') AS SELECT a FROM t""",
    """CREATE MATERIALIZED VIEW mv_refresh_every IN CLUSTER quickstart
        WITH (REFRESH EVERY '1 day' ALIGNED TO '2000-01-01 00:00:00+00')
        AS SELECT a FROM t""",
    """CREATE MATERIALIZED VIEW mv_refresh_many IN CLUSTER quickstart
        WITH (
            REFRESH AT '2999-06-01 00:00:00+00',
            REFRESH AT '2999-07-01 00:00:00+00',
            REFRESH EVERY '90 minutes' ALIGNED TO '2000-01-01 00:00:00+00'
        )
        AS SELECT a FROM t""",
    # A row for mz_replacements: a replacement that has not been applied.
    """CREATE REPLACEMENT MATERIALIZED VIEW mv_replacement FOR mv IN CLUSTER quickstart
        AS SELECT count(*) AS c FROM v WHERE a_abs > 0""",
    "CREATE TEMPORARY TABLE tmp_t (a int)",
    "CREATE TEMPORARY VIEW tmp_v AS SELECT * FROM t",
]
