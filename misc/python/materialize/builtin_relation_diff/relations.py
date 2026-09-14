# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The relations under test and their per-relation diff configuration."""

from collections.abc import Callable
from dataclasses import dataclass, field

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
    # Override the id namespace used to canonicalize a column. `dump` builds
    # the namespaces; `NAMESPACE_BY_COLUMN_NAME` picks the default for a column
    # name, and anything not listed there resolves in the "object" namespace.
    id_namespace_by_column: dict[str, str] = field(default_factory=dict)
    # Columns holding a textual array whose element order is insignificant:
    # the cell is rewritten with its elements sorted before diffing.
    sort_array_columns: list[str] = field(default_factory=list)
    # Whether the relation's builtin rows legitimately differ between versions
    # (builtin view definitions, builtin comments, builtin indexes). Under
    # --user-rows-only such a relation compares only rows whose id starts
    # with "u".
    builtin_rows_drift: bool = False


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
        builtin_rows_drift=True,
    ),
    "mz_catalog.mz_kafka_connections": RelationDiffConfig(),
    "mz_catalog.mz_kafka_sources": RelationDiffConfig(),
    "mz_catalog.mz_materialized_views": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster", "owner_id": "role"},
        sort_array_columns=["privileges"],
        builtin_rows_drift=True,
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
    # Both columns embed the connection's own catalog id, which whole-cell
    # canonicalization cannot reach inside a larger string.
    "mz_internal.mz_aws_connections": RelationDiffConfig(
        ignore_columns=["external_id", "example_trust_policy"],
    ),
    "mz_catalog.mz_aws_privatelink_connections": RelationDiffConfig(),
    "mz_internal.mz_cluster_schedules": RelationDiffConfig(
        id_namespace_by_column={"cluster_id": "cluster"},
    ),
    "mz_internal.mz_cluster_workload_classes": RelationDiffConfig(
        id_namespace_by_column={"id": "cluster"},
    ),
    "mz_internal.mz_comments": RelationDiffConfig(
        builtin_rows_drift=True,
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
    # `global_id` is canonicalized in its own namespace: a `GlobalId` and a
    # `CatalogItemId` with the same digits denote different objects, so
    # resolving both columns in the "object" namespace would rewrite the
    # mapping into nonsense. Several `GlobalId`s can share one item (one per
    # version of a table or materialized view), so those rows canonicalize to
    # the same pair and are distinguished only by their multiplicity, which
    # `one_sided` preserves.
    "mz_internal.mz_object_global_ids": RelationDiffConfig(
        builtin_rows_drift=True,
    ),
    "mz_internal.mz_pending_cluster_replicas": RelationDiffConfig(
        id_namespace_by_column={"id": "replica"},
    ),
    "mz_internal.mz_postgres_source_tables": RelationDiffConfig(),
    # The slot name carries a UUID generated per source at creation time.
    "mz_internal.mz_postgres_sources": RelationDiffConfig(
        ignore_columns=["replication_slot"],
    ),
    # `updated_at` records when the source last refreshed its references, so
    # it differs between the two environments by construction.
    "mz_internal.mz_source_references": RelationDiffConfig(
        ignore_columns=["updated_at"],
    ),
    "mz_internal.mz_sql_server_source_tables": RelationDiffConfig(),
}
