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
# policy creation is flag-gated on older versions, and workload classes are
# settable only by system users.
SYSTEM_CORPUS = [
    "ALTER SYSTEM SET enable_network_policies = true",
    "ALTER SYSTEM SET enable_alter_table_add_column = true",
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
    "CREATE TEMPORARY TABLE tmp_t (a int)",
    "CREATE TEMPORARY VIEW tmp_v AS SELECT * FROM t",
]
