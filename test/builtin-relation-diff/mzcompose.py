# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Validation harness for builtin-table-to-materialized-view migrations.

Dumps configured builtin relations from two fresh environments, one running a
baseline image and one running the locally built code, after applying the same
corpus of user objects to both, and diffs the results. A fresh environment's
builtin objects double as a large free test corpus.

System ids are not stable across builds (adding any builtin shifts
fresh-install id assignment), so dumps are canonicalized before diffing: any
cell whose value looks like a catalog id is rewritten to the qualified name of
the object it denotes on that side.

For an exact diff, run against the merge base of your branch:

    bin/mzcompose --find builtin-relation-diff run default \\
        --old-commit $(git merge-base HEAD origin/main)

Without --old-commit or --old-image the common-ancestor release image is
used. Builtins that were added or changed on main since that release then
show up as one-sided rows. Rows naming an object that does not exist on the
other side at all are tolerated automatically, but changes to a builtin
view's definition between the two versions are reported and need human
judgement.

To validate an already-shipped table-to-view conversion, diff against the
last release before the conversion version (see the MIGRATIONS list in
builtin_schema_migration.rs), restricted to the converted relations. Pass
--user-rows-only so relations whose builtin rows legitimately drift between
versions (builtin view definitions, builtin comments) only compare
user-created rows:

    bin/mzcompose --find builtin-relation-diff run default \\
        --old-image ghcr.io/materializeinc/materialize/materialized:vX.Y.Z \\
        --relation mz_catalog.mz_clusters --user-rows-only
"""

import argparse
import random
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from materialize.docker import commit_to_image_tag, image_registry
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import resolve_ancestor_image_tag
from materialize.workload_replay.config import (
    additional_system_parameter_defaults,
    cluster_replica_sizes,
)
from materialize.workload_replay.executor import test as replay_workload
from materialize.workload_replay.util import (
    get_paths,
    load_workload,
    update_captured_workloads_repo,
)

# One row of a canonicalized dump: column name -> canonicalized value.
Row = dict[str, str]
# Predicate deciding whether a one-sided row is an expected difference.
# Receives the row and the full canonicalized row lists of both sides.
AllowFn = Callable[[Row, list[Row], list[Row]], bool]


def is_dropped_element_ref_edge(
    row: Row, old_rows: list[Row], new_rows: list[Row]
) -> bool:
    """Expected old-only rows of mz_object_dependencies.

    Name resolution used to record the element type of an array type
    alongside the array type itself, so that `T[]` and `_T` produced the same
    ids. Both spellings now resolve to the array type alone, which retracts
    the element edge. An old-only edge to type X is therefore expected exactly
    when the same object also has an edge to the paired array type _X.

    This allowance can be deleted once the baseline the harness runs against
    is itself new enough to omit the element edge.
    """
    obj = row["object_id"]
    schema, sep, name = row["referenced_object_id"].rpartition(".")
    if not sep:
        return False
    array_ref = f"{schema}._{name}"
    return any(
        r["object_id"] == obj and r["referenced_object_id"] == array_ref
        for r in old_rows
    )


@dataclass
class RelationDiffConfig:
    """Per-relation knobs for the diff."""

    # Columns to drop before diffing (e.g. wall-clock timestamps).
    ignore_columns: list[str] = field(default_factory=list)
    # Expected rows present only in the baseline dump.
    allow_old_only: AllowFn | None = None
    # Expected rows present only in the new dump.
    allow_new_only: AllowFn | None = None
    # Override the id namespace used to canonicalize a column. The default
    # namespace for every id-shaped cell is "object"; other namespaces are
    # "cluster", "replica" and "role".
    id_namespace_by_column: dict[str, str] = field(default_factory=dict)
    # Columns holding a textual array whose element order is insignificant:
    # the cell is rewritten with its elements sorted before diffing.
    sort_array_columns: list[str] = field(default_factory=list)
    # WHERE clause selecting rows attributable to user objects. Applied only
    # when the workflow runs with --user-rows-only, for cross-version
    # baselines where builtin rows legitimately drift between versions.
    user_rows_where: str | None = None


# Covers every builtin-table-to-materialized-view conversion recorded in
# builtin_schema_migration.rs (the MIGRATIONS list). Relations the corpus
# cannot populate without external systems (Kafka, PostgreSQL, MySQL, SQL
# Server, AWS PrivateLink) are empty on both sides; their entries only
# validate the dump machinery against the relation's schema.
#
# The table-era populators sorted mz_aclitem arrays by grantee role id, while
# the converted views emit them in durable-JSON order. The contents are
# identical, so privileges columns use sort_array_columns.
RELATIONS: dict[str, RelationDiffConfig] = {
    # Wall-clock event times differ between the environments by construction.
    # Everything else, including the monotonic event ids, must match: both
    # sides run the identical bootstrap-plus-corpus DDL sequence.
    "mz_catalog.mz_audit_events": RelationDiffConfig(
        ignore_columns=["occurred_at"],
    ),
    "mz_catalog.mz_cluster_replicas": RelationDiffConfig(
        id_namespace_by_column={
            "id": "replica",
            "cluster_id": "cluster",
            "owner_id": "role",
        },
    ),
    "mz_catalog.mz_clusters": RelationDiffConfig(
        id_namespace_by_column={"id": "cluster", "owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_catalog.mz_connections": RelationDiffConfig(
        id_namespace_by_column={"owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_catalog.mz_databases": RelationDiffConfig(
        id_namespace_by_column={"owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_catalog.mz_default_privileges": RelationDiffConfig(
        id_namespace_by_column={"role_id": "role", "grantee": "role"},
    ),
    "mz_catalog.mz_indexes": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster", "owner_id": "role"},
        user_rows_where="id LIKE 'u%'",
    ),
    "mz_catalog.mz_kafka_connections": RelationDiffConfig(),
    "mz_catalog.mz_kafka_sources": RelationDiffConfig(),
    # Builtin view definitions drift between versions, hence user_rows_where.
    "mz_catalog.mz_materialized_views": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster", "owner_id": "role"},
        sort_array_columns=["privileges"],
        user_rows_where="id LIKE 'u%'",
    ),
    "mz_catalog.mz_role_members": RelationDiffConfig(
        id_namespace_by_column={
            "role_id": "role",
            "member": "role",
            "grantor": "role",
        },
    ),
    "mz_catalog.mz_role_parameters": RelationDiffConfig(
        id_namespace_by_column={"role_id": "role"},
    ),
    "mz_catalog.mz_roles": RelationDiffConfig(
        id_namespace_by_column={"id": "role"},
    ),
    "mz_catalog.mz_schemas": RelationDiffConfig(
        id_namespace_by_column={"owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_catalog.mz_secrets": RelationDiffConfig(
        id_namespace_by_column={"owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_catalog.mz_sources": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster", "owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    # The SSH keypair is generated randomly per environment.
    "mz_catalog.mz_ssh_tunnel_connections": RelationDiffConfig(
        ignore_columns=["public_key_1", "public_key_2"],
    ),
    "mz_catalog.mz_system_privileges": RelationDiffConfig(),
    "mz_internal.mz_aws_connections": RelationDiffConfig(),
    "mz_catalog.mz_aws_privatelink_connections": RelationDiffConfig(),
    "mz_internal.mz_cluster_schedules": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster"},
    ),
    "mz_internal.mz_cluster_workload_classes": RelationDiffConfig(
        id_namespace_by_column={"id": "cluster"},
    ),
    # Builtin comments drift between versions, hence user_rows_where.
    "mz_internal.mz_comments": RelationDiffConfig(
        user_rows_where="id LIKE 'u%'",
    ),
    "mz_internal.mz_internal_cluster_replicas": RelationDiffConfig(
        id_namespace_by_column={"id": "replica"},
    ),
    "mz_internal.mz_kafka_source_tables": RelationDiffConfig(),
    "mz_internal.mz_mysql_source_tables": RelationDiffConfig(),
    "mz_internal.mz_network_policies": RelationDiffConfig(
        id_namespace_by_column={"owner_id": "role"},
        sort_array_columns=["privileges"],
    ),
    "mz_internal.mz_network_policy_rules": RelationDiffConfig(),
    "mz_internal.mz_object_dependencies": RelationDiffConfig(
        allow_old_only=is_dropped_element_ref_edge,
    ),
    "mz_internal.mz_pending_cluster_replicas": RelationDiffConfig(
        id_namespace_by_column={"id": "replica"},
    ),
    "mz_internal.mz_postgres_source_tables": RelationDiffConfig(),
    "mz_internal.mz_postgres_sources": RelationDiffConfig(),
    "mz_internal.mz_sql_server_source_tables": RelationDiffConfig(),
}

# Statements that need the system account, applied before CORPUS: network
# policy creation is flag-gated on older versions, and workload classes are
# settable only by system users.
SYSTEM_CORPUS = [
    "ALTER SYSTEM SET enable_network_policies = true",
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

ID_PATTERN = re.compile(r"^(?:[ust]|si)\d+$")

# A canonicalized name: a namespaced cluster/replica/role value or a dotted
# qualified object name. Values that merely contain a dot but are not names
# (JSON details blobs, numbers, intervals) must not match, else one-sided
# rows containing them would be silently tolerated.
NAME_PATTERN = re.compile(
    r"^(?:cluster|replica|role):\S+$|^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$"
)

# `replay_workload` prints the console URL via `c.port("materialized", 6874)`,
# so the workload-mode service must publish the same ports the replay
# composition does, not just the SQL ports this harness reads.
# An AWS connection's external_id ends in the connection's own catalog id
# (mz_<environment-uuid>_s825). Whole-cell canonicalization cannot reach an id
# embedded in a larger string, and builtin ids shift whenever builtins are
# added, so the trailing id is scrubbed instead.
EXTERNAL_ID_PATTERN = re.compile(r"(mz_[0-9a-f-]{36})_[su]\d+")

# A Postgres source's replication slot name carries a freshly generated UUID,
# so it differs between the two sides for the same source.
SLOT_PATTERN = re.compile(r"materialize_[0-9a-f]{32}")

# The SSH bastion keypair is generated per environment, so a connection's
# stored PUBLIC KEY differs between the two sides by construction.
SSH_KEY_PATTERN = re.compile(r"ssh-ed25519 [A-Za-z0-9+/=]+")

# `AS OF <millis>` is stamped into the create_sql of a REFRESH materialized
# view from the wall clock at creation time, so it necessarily differs between
# two environments. Scrub the digits and keep the clause, so its presence and
# position still compare while the value does not.
AS_OF_PATTERN = re.compile(r"\bAS OF \d+")

# Columns whose ids live in a namespace other than "object", by convention.
# A relation's `id_namespace_by_column` overrides this.
NAMESPACE_BY_COLUMN_NAME = {
    "schema_id": "schema",
    "database_id": "database",
}

WORKLOAD_MZ_PORTS = [6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257]

SERVICES = [
    Materialized(name="mz_old"),  # Overridden below
    Materialized(name="mz_new"),  # Overridden below
    # Workload replay drives a single service named `materialized` (see
    # `workload_snapshot`), alongside the external systems a capture's
    # connections may reference. Mirrors test/workload-replay/mzcompose.py;
    # the shared config module keeps the sizes and parameters in step.
    Materialized(
        cluster_replica_size=cluster_replica_sizes,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
        ports=WORKLOAD_MZ_PORTS,
        environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
    ),
    # These mirror test/workload-replay/mzcompose.py rather than using
    # defaults. The replay framework creates Kafka topics from the host with
    # confluent_kafka.admin, so the broker needs a published host port and a
    # HOST advertised listener; Testdrive needs the vars the capture's
    # generated DDL references.
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        advertised_listeners=[
            "HOST://127.0.0.1:30123",
            "PLAINTEXT://kafka:9092",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Postgres(),
    MySql(),
    SqlServer(),
    SshBastionHost(allow_any_key=True),
    Testdrive(
        seed=1,
        no_reset=True,
        no_consistency_checks=True,
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        ],
    ),
    Mz(app_password=""),
]


@dataclass
class Snapshot:
    """Canonicalized dumps of one environment."""

    # relation -> canonicalized rows (sorted).
    dumps: dict[str, list[Row]]
    # All canonicalized names known to this environment (qualified object
    # names plus the cluster:, replica: and role: namespaces), used to
    # tolerate rows naming an object the other side does not have at all.
    known_names: set[str]


def snapshot(
    c: Composition,
    service: str,
    port: int,
    system_port: int,
    relations: list[str],
    user_rows_only: bool,
) -> Snapshot:
    """Apply CORPUS to one environment and dump the configured relations."""
    system_conn = c.sql_connection(service=service, port=system_port, user="mz_system")
    system_conn.autocommit = True
    system_cursor = system_conn.cursor()
    for stmt in SYSTEM_CORPUS:
        system_cursor.execute(stmt.encode())
    system_conn.close()

    conn = c.sql_connection(service=service, port=port)
    conn.autocommit = True
    cursor = conn.cursor()

    # The corpus runs on the same connection as the dumps so that temporary
    # items are alive while the relations are read.
    for stmt in CORPUS:
        cursor.execute(stmt.encode())

    try:
        return dump(cursor, relations, user_rows_only)
    finally:
        conn.close()


def dump(cursor: Any, relations: list[str], user_rows_only: bool) -> Snapshot:
    """Canonicalize and dump `relations` over an already-populated connection.

    Split out from `snapshot` so that workload replay, which populates the
    environment by a wholly different route, shares the canonicalization.
    """
    namespaces: dict[str, dict[str, str]] = {
        "object": {},
        "schema": {},
        "database": {},
        "cluster": {},
        "replica": {},
        "role": {},
    }
    cursor.execute(
        b"""
        SELECT o.id, coalesce(d.name || '.', '') || s.name || '.' || o.name
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        LEFT JOIN mz_databases d ON s.database_id = d.id
        """
    )
    namespaces["object"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'cluster:' || name FROM mz_clusters")
    namespaces["cluster"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(
        b"""
        SELECT r.id, 'replica:' || c.name || '.' || r.name
        FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id
        """
    )
    namespaces["replica"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'role:' || name FROM mz_roles")
    namespaces["role"] = {row[0]: row[1] for row in cursor.fetchall()}
    # Schemas and databases are not in mz_objects, so their ids must resolve in
    # their own namespaces. Sharing the object namespace silently maps a schema
    # id onto whichever unrelated object holds the same id, and because object
    # ids shift between builds the two sides then disagree on a row that is
    # actually identical.
    cursor.execute(
        b"""
        SELECT s.id, 'schema:' || coalesce(d.name || '.', '') || s.name
        FROM mz_schemas s LEFT JOIN mz_databases d ON s.database_id = d.id
        """
    )
    namespaces["schema"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'database:' || name FROM mz_databases")
    namespaces["database"] = {row[0]: row[1] for row in cursor.fetchall()}

    dumps = {}
    for relation, config in ((r, RELATIONS[r]) for r in relations):
        query = f"SELECT * FROM {relation}"
        if user_rows_only and config.user_rows_where:
            query += f" WHERE {config.user_rows_where}"
        cursor.execute(query.encode())
        columns = [d[0] for d in cursor.description]
        rows = []
        for raw in cursor.fetchall():
            row: Row = {}
            for column, value in zip(columns, raw):
                if column in config.ignore_columns:
                    continue
                value = AS_OF_PATTERN.sub("AS OF <TIMESTAMP>", str(value))
                value = SSH_KEY_PATTERN.sub("ssh-ed25519 <KEY>", value)
                value = SLOT_PATTERN.sub("materialize_<SLOT>", value)
                value = EXTERNAL_ID_PATTERN.sub(r"\1_<ID>", value)
                if ID_PATTERN.match(value):
                    namespace = config.id_namespace_by_column.get(
                        column, NAMESPACE_BY_COLUMN_NAME.get(column, "object")
                    )
                    value = namespaces[namespace].get(value, value)
                if (
                    column in config.sort_array_columns
                    and value.startswith("{")
                    and value.endswith("}")
                ):
                    value = "{" + ",".join(sorted(value[1:-1].split(","))) + "}"
                row[column] = value
            rows.append(row)
        rows.sort(key=lambda r: sorted(r.items()))
        dumps[relation] = rows

    known_names = set()
    for namespace in namespaces.values():
        known_names.update(namespace.values())
    return Snapshot(dumps=dumps, known_names=known_names)


def workload_snapshot(
    c: Composition,
    image: str | None,
    workload: dict[str, Any],
    workload_path: Any,
    relations: list[str],
    user_rows_only: bool,
    seed: str,
    verbose: bool,
) -> Snapshot:
    """Replay a captured workload on `image` and dump the configured relations.

    Only the object-creation phase runs: no initial data, no ingestion, no
    query load. The relations this harness diffs are catalog metadata, so the
    objects are the corpus and their contents are irrelevant.

    `replay_workload` brings up a service named `materialized` itself, so the
    two sides run sequentially here rather than side by side as in corpus
    mode. The dump is taken from `during_continuous`, which the replay invokes
    once the objects exist and have hydrated.

    The seed is pinned rather than defaulted to the clock: a diff between two
    builds is meaningless if the corpus differs between them.
    """
    random.seed(seed)
    captured: dict[str, Snapshot] = {}

    def capture() -> None:
        conn = c.sql_connection(service="materialized", port=6875)
        conn.autocommit = True
        try:
            captured["snapshot"] = dump(conn.cursor(), relations, user_rows_only)
        finally:
            conn.close()

    with c.override(
        Materialized(
            image=image,
            cluster_replica_size=cluster_replica_sizes,
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            ports=WORKLOAD_MZ_PORTS,
            environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
            use_default_volumes=False,
        )
    ):
        replay_workload(
            c,
            workload,
            workload_path,
            factor_initial_data=1,
            factor_ingestions=1,
            factor_queries=1,
            runtime=0,
            verbose=verbose,
            create_objects=True,
            initial_data=False,
            early_initial_data=False,
            run_ingestions=False,
            run_queries=False,
            max_concurrent_queries=1,
            during_continuous=capture,
        )

    if "snapshot" not in captured:
        raise AssertionError(
            "workload replay finished without reaching the dump callback"
        )
    return captured["snapshot"]


def one_sided(rows: list[Row], other: list[Row]) -> list[Row]:
    remaining = list(other)
    result = []
    for row in rows:
        if row in remaining:
            remaining.remove(row)
        else:
            result.append(row)
    return result


def names_object_absent_from(row: Row, known_names: set[str]) -> bool:
    return any(
        NAME_PATTERN.match(value) and value not in known_names for value in row.values()
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--old-image",
        type=str,
        default=None,
        help="baseline materialized image; defaults to the common-ancestor release",
    )
    parser.add_argument(
        "--old-commit",
        type=str,
        default=None,
        help="commit hash to resolve the baseline image from "
        "(use the merge base of your branch for an exact diff)",
    )
    parser.add_argument(
        "--relation",
        action="append",
        choices=sorted(RELATIONS),
        help="relation to diff (default: all configured relations)",
    )
    parser.add_argument(
        "--user-rows-only",
        action="store_true",
        help="for relations configuring it, compare only user-created rows; "
        "use when builtin rows legitimately drift between the two versions",
    )
    parser.add_argument(
        "--workload",
        type=str,
        default=None,
        help="replay this captured workload as the corpus instead of CORPUS, "
        "e.g. 'workload_prod_sandbox' (see test/workload-replay/README.md). "
        "Richer, but needs the captured-workloads repo and external systems, "
        "and cannot cover temporary items",
    )
    parser.add_argument(
        "--workload-seed",
        type=str,
        default="builtin-relation-diff",
        help="seed for workload replay; both sides use it, so changing it "
        "changes the corpus but never introduces a difference between builds",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    args: argparse.Namespace = parser.parse_args()

    relations = args.relation or sorted(RELATIONS)
    old_image = args.old_image
    if old_image is None and args.old_commit is not None:
        tag = commit_to_image_tag(args.old_commit)
        old_image = f"{image_registry()}/materialized:{tag}"
    if old_image is None:
        tag = resolve_ancestor_image_tag(ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS)
        old_image = f"{image_registry()}/materialized:{tag}"
    print(f"Baseline image: {old_image}")

    c.down(destroy_volumes=True)

    sql_port = 6875
    system_port = 6877

    if args.workload:
        # Replay drives one `materialized` service, so the sides run one after
        # the other, each against a freshly reset environment.
        update_captured_workloads_repo()
        matches = get_paths([f"{args.workload}.yml"])
        if len(matches) != 1:
            raise UIError(
                f"--workload {args.workload!r} matched {len(matches)} capture files; "
                "pass the file's basename without the .yml suffix"
            )
        workload_path = matches[0]
        workload = load_workload(workload_path)
        print(f"Corpus: replay of {workload_path.name}")

        snapshots = []
        for label, image in (("baseline", old_image), ("new", None)):
            print(f"--- Replaying workload on the {label} build")
            snapshots.append(
                workload_snapshot(
                    c,
                    image,
                    workload,
                    workload_path,
                    relations,
                    args.user_rows_only,
                    args.workload_seed,
                    bool(args.verbose),
                )
            )
            c.down(destroy_volumes=True)
        old, new = snapshots
    else:
        with c.override(
            Materialized(
                name="mz_old",
                image=old_image,
                ports=[f"16875:{sql_port}", f"16877:{system_port}"],
                use_default_volumes=False,
            ),
            Materialized(
                name="mz_new",
                image=None,
                ports=[f"26875:{sql_port}", f"26877:{system_port}"],
                use_default_volumes=False,
            ),
        ):
            c.up("mz_old", "mz_new")
            old = snapshot(
                c, "mz_old", sql_port, system_port, relations, args.user_rows_only
            )
            new = snapshot(
                c, "mz_new", sql_port, system_port, relations, args.user_rows_only
            )

    failures = []
    for relation in relations:
        config = RELATIONS[relation]
        old_rows = old.dumps[relation]
        new_rows = new.dumps[relation]

        old_only = one_sided(old_rows, new_rows)
        new_only = one_sided(new_rows, old_rows)

        unexplained = []
        for row in old_only:
            if config.allow_old_only and config.allow_old_only(row, old_rows, new_rows):
                continue
            if names_object_absent_from(row, new.known_names):
                print(
                    f"{relation}: tolerating old-only row naming an object absent from the new build: {row}"
                )
                continue
            unexplained.append(("old-only", row))
        for row in new_only:
            if config.allow_new_only and config.allow_new_only(row, old_rows, new_rows):
                continue
            if names_object_absent_from(row, old.known_names):
                print(
                    f"{relation}: tolerating new-only row naming an object absent from the baseline: {row}"
                )
                continue
            unexplained.append(("new-only", row))

        print(
            f"{relation}: {len(old_rows)} baseline rows, {len(new_rows)} new rows, "
            f"{len(old_only)} old-only, {len(new_only)} new-only, "
            f"{len(unexplained)} unexplained"
        )
        if unexplained:
            for side, row in unexplained:
                print(f"{relation}: UNEXPLAINED {side}: {row}")
            failures.append(relation)

    if failures:
        raise AssertionError(f"unexplained differences in: {', '.join(failures)}")
