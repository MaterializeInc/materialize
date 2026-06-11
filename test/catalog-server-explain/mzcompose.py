# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Keeps `test/sqllogictest/catalog_server_explain.slt` in sync with the set of
objects maintained or defined on the `mz_catalog_server` cluster.

That .slt file holds one `EXPLAIN` query per builtin index, materialized view,
and system-schema view, so that any change to builtin catalog SQL surfaces the
full set of downstream plan changes directly in a PR diff (see the header of
the .slt file for the rationale).

This composition owns the *list of EXPLAIN queries* (which objects are covered)
by enumerating the catalog. sqllogictest itself owns the *expected plans* (run
`bin/sqllogictest --rewrite-results` to fill them in).

* `run default` (CI): assert that the .slt covers exactly the current set of
  catalog-server objects. Fails, listing the offending objects, if a builtin
  was added or removed without regenerating the query list.
* `run default --rewrite`: regenerate the query list in the .slt, preserving
  the already-recorded plans for queries that still exist.
"""

from __future__ import annotations

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.ui import UIError

SLT_PATH = MZ_ROOT / "test" / "sqllogictest" / "catalog_server_explain.slt"

BEGIN_MARKER = "# BEGIN AUTO-GENERATED QUERIES"
END_MARKER = "# END AUTO-GENERATED QUERIES"

CATALOG_SERVER_CLUSTER = "mz_catalog_server"

# System schemas whose views we snapshot. Mirrors `SYSTEM_SCHEMAS` in
# src/repr/src/namespaces.rs. Views are not tied to a cluster, but when planned
# with `SET cluster = mz_catalog_server` (as the .slt does) they resolve the
# arrangements maintained on the catalog server, which is exactly the plan we
# want to guard against churn.
SYSTEM_SCHEMAS = [
    "mz_catalog",
    "mz_catalog_unstable",
    "pg_catalog",
    "mz_internal",
    "mz_introspection",
    "information_schema",
    "mz_unsafe",
]

# Enumerate builtins from a stock `Materialized`. Its system-parameter defaults
# come from `get_default_system_parameters()` -- the very function that
# sqllogictest's embedded server uses to fill the plans (see
# misc/python/materialize/cli/run.py). Both servers therefore enable the same
# flag-gated builtins, so the object set enumerated here matches exactly what
# the plan-filling server can EXPLAIN. Do not pass
# `additional_system_parameter_defaults`: any override here that the
# sqllogictest server does not also apply would desync the two.
SERVICES = [Materialized()]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--rewrite",
        action="store_true",
        help="Rewrite the auto-generated query list in the .slt file instead of"
        " validating it.",
    )
    args = parser.parse_args()

    c.up("materialized")

    queries = enumerate_explain_queries(c)

    if args.rewrite:
        rewrite_slt(queries)
        print(f"Wrote {len(queries)} EXPLAIN queries to {SLT_PATH}.")
        print(
            "Now run `bin/sqllogictest --rewrite-results` on the file to fill in"
            " the plans."
        )
    else:
        validate_slt(queries)
        print(f"OK: {SLT_PATH} covers all {len(queries)} catalog-server objects.")


def _quote_ident(ident: str) -> str:
    """Double-quote a SQL identifier, escaping embedded double quotes."""
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _qualified(schema: str, name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


def enumerate_explain_queries(c: Composition) -> list[str]:
    """Return the deterministically-ordered list of EXPLAIN statements that
    should appear in the .slt, one per catalog-server object."""
    # Exclude per-replica introspection log indexes (catalog kind
    # `ClusterIntrospectionSourceIndex`, surfaced with an `si` id prefix; see
    # `introspection_source_indexes_cte` in src/catalog/src/builtin/mz_catalog.rs).
    # These are auto-created once per replica over the logging arrangements, so
    # their names embed a non-deterministic replica id (e.g.
    # `..._s2_primary_idx`) and they have no optimizer plan -- `EXPLAIN INDEX`
    # errors with "cannot find dataflow metainformation". The `si`-prefix filter
    # keys off the object kind, not the schema, so it still excludes them if a
    # future log index is created over a relation outside `mz_introspection`.
    indexes = c.sql_query(f"""
        SELECT s.name, o.name
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        JOIN mz_clusters c ON o.cluster_id = c.id
        WHERE o.type = 'index' AND c.name = '{CATALOG_SERVER_CLUSTER}'
          AND o.id NOT LIKE 'si%'
        ORDER BY s.name, o.name
        """)
    materialized_views = c.sql_query(f"""
        SELECT s.name, o.name
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        JOIN mz_clusters c ON o.cluster_id = c.id
        WHERE o.type = 'materialized-view' AND c.name = '{CATALOG_SERVER_CLUSTER}'
        ORDER BY s.name, o.name
        """)
    schema_list = ", ".join(f"'{s}'" for s in SYSTEM_SCHEMAS)
    views = c.sql_query(f"""
        SELECT s.name, v.name
        FROM mz_views v
        JOIN mz_schemas s ON v.schema_id = s.id
        WHERE s.name IN ({schema_list})
        ORDER BY s.name, v.name
        """)

    queries: list[str] = []
    for schema, name in indexes:
        queries.append(f"EXPLAIN INDEX {_qualified(schema, name)};")
    for schema, name in materialized_views:
        queries.append(f"EXPLAIN MATERIALIZED VIEW {_qualified(schema, name)};")
    for schema, name in views:
        queries.append(f"EXPLAIN SELECT * FROM {_qualified(schema, name)};")
    return queries


def _split_file() -> tuple[list[str], list[str], list[str]]:
    """Split the .slt into (header_through_begin_marker, records_lines,
    end_marker_through_tail)."""
    text = SLT_PATH.read_text()
    lines = text.split("\n")
    try:
        begin = next(i for i, l in enumerate(lines) if l.startswith(BEGIN_MARKER))
        end = next(i for i, l in enumerate(lines) if l.startswith(END_MARKER))
    except StopIteration:
        raise UIError(
            f"{SLT_PATH} is missing the {BEGIN_MARKER!r} / {END_MARKER!r} markers."
        )
    return lines[: begin + 1], lines[begin + 1 : end], lines[end:]


def _parse_records(records_lines: list[str]) -> dict[str, list[str]]:
    """Parse the auto-generated region into a mapping of EXPLAIN statement to
    its recorded plan output lines (so plans survive a query-list rewrite)."""
    plans: dict[str, list[str]] = {}
    i = 0
    while i < len(records_lines):
        if records_lines[i].strip() == "query T multiline":
            sql = records_lines[i + 1]
            assert records_lines[i + 2] == "----", f"malformed record near: {sql!r}"
            j = i + 3
            output: list[str] = []
            while j < len(records_lines) and records_lines[j] != "EOF":
                output.append(records_lines[j])
                j += 1
            plans[sql] = output
            i = j + 1
        else:
            i += 1
    return plans


def _render_records(queries: list[str], plans: dict[str, list[str]]) -> str:
    blocks = []
    for sql in queries:
        output = plans.get(sql, [])
        body = "".join(f"{line}\n" for line in output)
        blocks.append(f"query T multiline\n{sql}\n----\n{body}EOF")
    return "\n\n".join(blocks)


def rewrite_slt(queries: list[str]) -> None:
    head, records_lines, tail = _split_file()
    plans = _parse_records(records_lines)
    rendered = _render_records(queries, plans)
    body = ["", rendered, ""] if rendered else [""]
    SLT_PATH.write_text("\n".join(head + body + tail))


def validate_slt(queries: list[str]) -> None:
    _, records_lines, _ = _split_file()
    covered = set(_parse_records(records_lines).keys())
    desired = set(queries)

    missing = sorted(desired - covered)
    extra = sorted(covered - desired)
    if not missing and not extra:
        return

    lines = [
        f"{SLT_PATH} is out of date with the objects on {CATALOG_SERVER_CLUSTER}.",
        "Regenerate it with:",
        "  bin/mzcompose --find catalog-server-explain run default --rewrite",
        "  bin/sqllogictest --rewrite-results test/sqllogictest/catalog_server_explain.slt",
    ]
    if missing:
        lines.append(f"\nMissing EXPLAIN queries ({len(missing)}):")
        lines.extend(f"  + {q}" for q in missing)
    if extra:
        lines.append(
            f"\nStale EXPLAIN queries no longer backed by an object ({len(extra)}):"
        )
        lines.extend(f"  - {q}" for q in extra)
    raise UIError("\n".join(lines))
