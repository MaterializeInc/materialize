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
"""

import argparse
import re
from collections.abc import Callable
from dataclasses import dataclass, field

from materialize.docker import commit_to_image_tag, image_registry
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import resolve_ancestor_image_tag

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


RELATIONS: dict[str, RelationDiffConfig] = {
    "mz_internal.mz_object_dependencies": RelationDiffConfig(
        allow_old_only=is_dropped_element_ref_edge,
    ),
}

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
    "CREATE TEMPORARY TABLE tmp_t (a int)",
    "CREATE TEMPORARY VIEW tmp_v AS SELECT * FROM t",
]

ID_PATTERN = re.compile(r"^(?:[ust]|si)\d+$")

SERVICES = [
    Materialized(name="mz_old"),  # Overridden below
    Materialized(name="mz_new"),  # Overridden below
    Mz(app_password=""),
]


@dataclass
class Snapshot:
    """Canonicalized dumps of one environment."""

    # relation -> canonicalized rows (sorted).
    dumps: dict[str, list[Row]]
    # All qualified object names known to this environment, used to tolerate
    # rows naming an object the other side does not have at all.
    object_names: set[str]


def snapshot(c: Composition, service: str, port: int, relations: list[str]) -> Snapshot:
    conn = c.sql_connection(service=service, port=port)
    conn.autocommit = True
    cursor = conn.cursor()

    # The corpus runs on the same connection as the dumps so that temporary
    # items are alive while the relations are read.
    for stmt in CORPUS:
        cursor.execute(stmt.encode())

    namespaces: dict[str, dict[str, str]] = {
        "object": {},
        "cluster": {},
        "replica": {},
        "role": {},
    }
    cursor.execute(b"""
        SELECT o.id, coalesce(d.name || '.', '') || s.name || '.' || o.name
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        LEFT JOIN mz_databases d ON s.database_id = d.id
        """)
    namespaces["object"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'cluster:' || name FROM mz_clusters")
    namespaces["cluster"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"""
        SELECT r.id, 'replica:' || c.name || '.' || r.name
        FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id
        """)
    namespaces["replica"] = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute(b"SELECT id, 'role:' || name FROM mz_roles")
    namespaces["role"] = {row[0]: row[1] for row in cursor.fetchall()}

    dumps = {}
    for relation, config in ((r, RELATIONS[r]) for r in relations):
        cursor.execute(f"SELECT * FROM {relation}".encode())
        columns = [d[0] for d in cursor.description]
        rows = []
        for raw in cursor.fetchall():
            row: Row = {}
            for column, value in zip(columns, raw):
                if column in config.ignore_columns:
                    continue
                value = str(value)
                if ID_PATTERN.match(value):
                    namespace = config.id_namespace_by_column.get(column, "object")
                    value = namespaces[namespace].get(value, value)
                row[column] = value
            rows.append(row)
        rows.sort(key=lambda r: sorted(r.items()))
        dumps[relation] = rows

    conn.close()
    return Snapshot(dumps=dumps, object_names=set(namespaces["object"].values()))


def one_sided(rows: list[Row], other: list[Row]) -> list[Row]:
    remaining = list(other)
    result = []
    for row in rows:
        if row in remaining:
            remaining.remove(row)
        else:
            result.append(row)
    return result


def names_object_absent_from(row: Row, object_names: set[str]) -> bool:
    return any(
        "." in value and value not in object_names and not ID_PATTERN.match(value)
        for value in row.values()
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

    internal_sql_port = 6875
    with c.override(
        Materialized(
            name="mz_old",
            image=old_image,
            ports=[f"16875:{internal_sql_port}"],
            use_default_volumes=False,
        ),
        Materialized(
            name="mz_new",
            image=None,
            ports=[f"26875:{internal_sql_port}"],
            use_default_volumes=False,
        ),
    ):
        c.up("mz_old", "mz_new")
        old = snapshot(c, "mz_old", internal_sql_port, relations)
        new = snapshot(c, "mz_new", internal_sql_port, relations)

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
            if names_object_absent_from(row, new.object_names):
                print(
                    f"{relation}: tolerating old-only row naming an object absent from the new build: {row}"
                )
                continue
            unexplained.append(("old-only", row))
        for row in new_only:
            if config.allow_new_only and config.allow_new_only(row, old_rows, new_rows):
                continue
            if names_object_absent_from(row, old.object_names):
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
