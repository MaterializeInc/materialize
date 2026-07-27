# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Catalog queries: object resolution, dependency walking, index discovery."""

from dataclasses import dataclass

from materialize.mzbisect.db import Db, ident, literal

# Object types that hold data and can be probed with SELECT. Everything else
# in the dependency closure (connections, secrets, sinks, ...) is skipped.
RELATION_TYPES = ("table", "source", "view", "materialized-view")


@dataclass(frozen=True)
class ObjectInfo:
    id: str
    name: str
    type: str
    schema: str
    database: str | None

    @property
    def qualified(self) -> str:
        """The fully qualified, quoted name for use in FROM clauses."""
        parts = [] if self.database is None else [ident(self.database)]
        parts += [ident(self.schema), ident(self.name)]
        return ".".join(parts)

    @property
    def display(self) -> str:
        parts = [] if self.database is None else [self.database]
        parts += [self.schema, self.name]
        return ".".join(parts)

    @property
    def is_system(self) -> bool:
        return self.id.startswith("s")


@dataclass(frozen=True)
class IndexInfo:
    id: str
    name: str
    cluster_id: str
    cluster_name: str


@dataclass(frozen=True)
class ReplicaInfo:
    id: str
    name: str


class UnresolvableObject(Exception):
    pass


_OBJECT_COLUMNS = """
    o.id,
    o.name,
    o.type,
    s.name AS schema_name,
    d.name AS database_name
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
LEFT JOIN mz_catalog.mz_databases d ON s.database_id = d.id
"""


def _object_from_row(row: dict) -> ObjectInfo:
    return ObjectInfo(
        id=row["id"],
        name=row["name"],
        type=row["type"],
        schema=row["schema_name"],
        database=row["database_name"],
    )


def resolve_object(db: Db, spec: str) -> ObjectInfo:
    """Resolve an object ID or (partially) qualified name to an ObjectInfo.

    Accepts `u123`, `name`, `schema.name`, or `database.schema.name`. Raises
    UnresolvableObject if the spec matches zero or more than one object.
    """
    if spec.startswith("u") and spec[1:].isdigit():
        where = f"o.id = {literal(spec)}"
    else:
        parts = spec.split(".")
        if len(parts) == 1:
            where = f"o.name = {literal(parts[0])}"
        elif len(parts) == 2:
            where = f"s.name = {literal(parts[0])} AND o.name = {literal(parts[1])}"
        elif len(parts) == 3:
            where = (
                f"d.name = {literal(parts[0])}"
                f" AND s.name = {literal(parts[1])}"
                f" AND o.name = {literal(parts[2])}"
            )
        else:
            raise UnresolvableObject(f"malformed object name: {spec}")

    rows = db.query(f"SELECT {_OBJECT_COLUMNS} WHERE {where}")
    if not rows:
        raise UnresolvableObject(f"no object matches {spec!r}")
    if len(rows) > 1:
        candidates = ", ".join(
            f"{_object_from_row(r).display} ({r['id']})" for r in rows
        )
        raise UnresolvableObject(
            f"{spec!r} is ambiguous, qualify further or use an ID: {candidates}"
        )
    return _object_from_row(rows[0])


def resolve_index_target(db: Db, obj: ObjectInfo) -> ObjectInfo:
    """If obj is an index, return the indexed relation, else obj itself."""
    if obj.type != "index":
        return obj
    row = db.query_one(
        f"SELECT on_id FROM mz_catalog.mz_indexes WHERE id = {literal(obj.id)}"
    )
    return resolve_object(db, row["on_id"])


def direct_dependencies(db: Db, obj: ObjectInfo) -> list[ObjectInfo]:
    """The relations obj directly depends on, in stable order."""
    rows = db.query(f"""
        SELECT DISTINCT {_OBJECT_COLUMNS}
        JOIN mz_internal.mz_object_dependencies dep
            ON dep.referenced_object_id = o.id
        WHERE dep.object_id = {literal(obj.id)}
          AND o.type IN ({", ".join(literal(t) for t in RELATION_TYPES)})
        ORDER BY o.id
        """)
    return [_object_from_row(r) for r in rows]


def indexes_on(db: Db, obj: ObjectInfo) -> list[IndexInfo]:
    rows = db.query(f"""
        SELECT i.id, i.name, i.cluster_id, c.name AS cluster_name
        FROM mz_catalog.mz_indexes i
        JOIN mz_catalog.mz_clusters c ON c.id = i.cluster_id
        WHERE i.on_id = {literal(obj.id)}
        ORDER BY i.id
        """)
    return [
        IndexInfo(
            id=r["id"],
            name=r["name"],
            cluster_id=r["cluster_id"],
            cluster_name=r["cluster_name"],
        )
        for r in rows
    ]


def has_columns(db: Db, obj: ObjectInfo) -> bool:
    """Whether the relation has any columns.

    Zero-column relations exist (e.g. the parent object of a Postgres
    source) and `row(t.*)` cannot be written against them, so the row-shaped
    probes do not apply.
    """
    row = db.query_one(
        f"SELECT count(*) AS n FROM mz_catalog.mz_columns"
        f" WHERE id = {literal(obj.id)}"
    )
    return int(row["n"]) > 0


def cluster_replicas(db: Db, cluster_id: str) -> list[ReplicaInfo]:
    rows = db.query(f"""
        SELECT id, name
        FROM mz_catalog.mz_cluster_replicas
        WHERE cluster_id = {literal(cluster_id)}
        ORDER BY id
        """)
    return [ReplicaInfo(id=r["id"], name=r["name"]) for r in rows]


def compute_error_candidates(db: Db) -> list[dict]:
    """Persistent user dataflows currently reporting nonzero error counts.

    An entry here is a candidate for bisection but not proof of corruption:
    the counts include legitimate dataflow errors (division by zero and
    friends) alongside consistency violations.
    """
    return db.query("""
        SELECT
            ec.object_id,
            o.name AS object_name,
            o.type AS object_type,
            i.on_id AS indexed_object_id,
            cl.name AS cluster_name,
            r.name AS replica_name,
            ec.count::text AS error_count
        FROM mz_internal.mz_compute_error_counts_raw_unified ec
        LEFT JOIN mz_catalog.mz_objects o ON o.id = ec.object_id
        LEFT JOIN mz_catalog.mz_indexes i ON i.id = ec.object_id
        LEFT JOIN mz_catalog.mz_cluster_replicas r ON r.id = ec.replica_id
        LEFT JOIN mz_catalog.mz_clusters cl ON cl.id = r.cluster_id
        WHERE ec.object_id LIKE 'u%' AND ec.count != 0
        ORDER BY ec.object_id, ec.replica_id
        """)
