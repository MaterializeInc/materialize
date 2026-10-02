# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Canonicalized dumps of the configured relations and their comparison."""

import re
from dataclasses import dataclass
from typing import Any

from materialize.builtin_relation_diff.relations import RELATIONS, Row

ID_PATTERN = re.compile(r"^(?:[ust]|si)\d+$")

# A canonicalized name: a namespaced cluster/replica/role value or a dotted
# qualified object name. Values that merely contain a dot but are not names
# (JSON details blobs, numbers, intervals) must not match, else one-sided
# rows containing them would be silently tolerated.
NAME_PATTERN = re.compile(
    r"^(?:cluster|replica|role):\S+$|^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$"
)

# `AS OF <millis>` is stamped into the create_sql of a REFRESH materialized
# view from the wall clock at creation time, so it necessarily differs between
# two environments. Scrub the digits and keep the clause, so its presence and
# position still compare while the value does not.
AS_OF_PATTERN = re.compile(r"\bAS OF \d+")

# A `[<id> AS <name>]` reference inside a create_sql already carries the name,
# and a system id there shifts whenever a builtin is added ahead of the object
# it denotes, so only the name is kept.
ID_REF_PATTERN = re.compile(r"\[[ust]\d+ AS ")

# A Postgres source's replication slot name carries a UUID generated per source
# at creation time. Beyond mz_postgres_sources.replication_slot, which is
# dropped outright, it reaches the dump hex-encoded inside the protobuf
# `DETAILS` option of the source's create_sql. "materialize_" encodes to
# 6d6174657269616c697a655f, followed by the UUID's 32 hex characters, each
# themselves ASCII-hex-encoded, so 64 digits.
SLOT_HEX_PATTERN = re.compile(r"6d6174657269616c697a655f[0-9a-f]{64}")

# An SSH tunnel connection's keypair is generated per environment. Beyond the
# public key columns of mz_ssh_tunnel_connections, which are dropped outright,
# the keys reach the dump inside the connection's create_sql.
SSH_KEY_PATTERN = re.compile(r"ssh-ed25519 [A-Za-z0-9+/=]+")

# Columns whose ids live in a namespace other than "object", by convention.
# A relation's `id_namespace_by_column` overrides this.
NAMESPACE_BY_COLUMN_NAME = {
    "schema_id": "schema",
    "database_id": "database",
    "global_id": "global_id",
}


@dataclass
class Snapshot:
    """Canonicalized dumps of one environment."""

    # relation -> canonicalized rows (sorted).
    dumps: dict[str, list[Row]]
    # All canonicalized names known to this environment (qualified object
    # names plus the cluster:, replica: and role: namespaces), used to
    # tolerate rows naming an object the other side does not have at all.
    known_names: set[str]
    # Qualified object name -> `mz_objects.type`. A builtin that is a table on
    # one side and a materialized view on the other is two different objects
    # sharing a name, so rows about it are one-sided by construction.
    object_kinds: dict[str, str]


def dump(cursor: Any, relations: list[str], user_rows_only: bool) -> Snapshot:
    """Canonicalize and dump `relations` over an already-populated connection."""
    namespaces: dict[str, dict[str, str]] = {
        "object": {},
        "schema": {},
        "database": {},
        "cluster": {},
        "replica": {},
        "role": {},
        "global_id": {},
    }
    cursor.execute(b"""
        SELECT o.id, coalesce(d.name || '.', '') || s.name || '.' || o.name, o.type
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        LEFT JOIN mz_databases d ON s.database_id = d.id
        """)
    objects = cursor.fetchall()
    namespaces["object"] = {row[0]: row[1] for row in objects}
    object_kinds = {row[1]: row[2] for row in objects}
    cursor.execute(b"SELECT id, 'cluster:' || name FROM mz_clusters")
    namespaces["cluster"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"""
        SELECT r.id, 'replica:' || c.name || '.' || r.name
        FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id
        """)
    namespaces["replica"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'role:' || name FROM mz_roles")
    namespaces["role"] = {row[0]: row[1] for row in cursor.fetchall()}
    # Schemas and databases are not in mz_objects, so their ids must resolve in
    # their own namespaces. Sharing the object namespace silently maps a schema
    # id onto whichever unrelated object holds the same id, and because object
    # ids shift between builds the two sides then disagree on a row that is
    # actually identical.
    cursor.execute(b"""
        SELECT s.id, 'schema:' || coalesce(d.name || '.', '') || s.name
        FROM mz_schemas s LEFT JOIN mz_databases d ON s.database_id = d.id
        """)
    namespaces["schema"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'database:' || name FROM mz_databases")
    namespaces["database"] = {row[0]: row[1] for row in cursor.fetchall()}
    # `GlobalId`s resolve through the relation under test, the same way cluster
    # and role ids resolve through `mz_clusters` and `mz_roles`, which are also
    # diffed. A mapping that is wrong on one side canonicalizes that side's row
    # differently and so still shows up as a difference.
    cursor.execute(b"""
        SELECT g.global_id, coalesce(d.name || '.', '') || s.name || '.' || o.name
        FROM mz_internal.mz_object_global_ids g
        JOIN mz_objects o ON o.id = g.id
        JOIN mz_schemas s ON o.schema_id = s.id
        LEFT JOIN mz_databases d ON s.database_id = d.id
        """)
    namespaces["global_id"] = {row[0]: row[1] for row in cursor.fetchall()}

    dumps = {}
    for relation, config in ((r, RELATIONS[r]) for r in relations):
        query = f"SELECT * FROM {relation}"
        if user_rows_only and config.builtin_rows_drift:
            query += f" WHERE {config.id_column} LIKE 'u%'"
        cursor.execute(query.encode())
        columns = [d[0] for d in cursor.description]
        rows = []
        for raw in cursor.fetchall():
            row: Row = {}
            for column, value in zip(columns, raw):
                if column in config.ignore_columns:
                    continue
                value = AS_OF_PATTERN.sub("AS OF <TIMESTAMP>", str(value))
                value = ID_REF_PATTERN.sub("[<id> AS ", value)
                value = SLOT_HEX_PATTERN.sub("<SLOT_HEX>", value)
                value = SSH_KEY_PATTERN.sub("<SSH_KEY>", value)
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
    return Snapshot(dumps=dumps, known_names=known_names, object_kinds=object_kinds)


def one_sided(rows: list[Row], other: list[Row]) -> list[Row]:
    remaining = list(other)
    result = []
    for row in rows:
        if row in remaining:
            remaining.remove(row)
        else:
            result.append(row)
    return result


def names_object_absent_from(row: Row, this: Snapshot, other: Snapshot) -> bool:
    """Whether `row`, from `this` side, names an object the other side lacks.

    An object of another kind on the other side counts as lacking: a builtin
    table that became a materialized view keeps its name, but every row about
    it (its own catalog rows, its dependency edges, its comments) belongs to
    the new object and has no counterpart on the other side.
    """
    for value in row.values():
        if not NAME_PATTERN.match(value):
            continue
        if value not in other.known_names:
            return True
        kind = this.object_kinds.get(value)
        if kind is not None and other.object_kinds.get(value) != kind:
            return True
    return False


def compare(relations: list[str], old: Snapshot, new: Snapshot) -> list[str]:
    """Report the differences per relation and return those with unexplained rows."""
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
            if names_object_absent_from(row, old, new):
                print(
                    f"{relation}: tolerating old-only row naming an object absent from, or of another kind on, the new build: {row}"
                )
                continue
            unexplained.append(("old-only", row))
        for row in new_only:
            if config.allow_new_only and config.allow_new_only(row, old_rows, new_rows):
                continue
            if names_object_absent_from(row, new, old):
                print(
                    f"{relation}: tolerating new-only row naming an object absent from, or of another kind on, the baseline: {row}"
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
    return failures
