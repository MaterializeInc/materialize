# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT


def _locate_redactor() -> list[str] | None:
    """Locate the mz-sql-anonymize helper binary, if it has been built.

    Honors MZ_SQL_ANONYMIZE_BIN, then looks for a release or debug build in the
    Cargo target directory. Returns the argv prefix to run it, or None.
    """
    override = os.environ.get("MZ_SQL_ANONYMIZE_BIN")
    if override and Path(override).exists():
        return [override]
    for profile in ("release", "debug"):
        candidate = MZ_ROOT / "target" / profile / "mz-sql-anonymize"
        if candidate.exists():
            return [str(candidate)]
    return None


def redact_literals_via_parser(sqls: list[str]) -> list[str | None] | None:
    """Redact literals in each SQL string using Materialize's own parser.

    Returns a list aligned with the input, where each element is the redacted
    SQL or None if that statement could not be parsed. Returns None for the
    whole batch if the helper binary is unavailable or errors, signaling the
    caller to fall back to regex-based redaction.
    """
    cmd = _locate_redactor()
    if cmd is None:
        return None
    proc = subprocess.run(
        cmd,
        input=json.dumps(sqls),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(
            f"warning: {cmd[0]} failed, falling back to regex redaction:\n{proc.stderr}",
            file=sys.stderr,
        )
        return None
    return json.loads(proc.stdout)


def keywords() -> set[str]:
    with open(MZ_ROOT / "src" / "sql-lexer" / "src" / "keywords.txt") as f:
        result = set(
            line.strip().lower()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )
    # On a fresh Materialize: SELECT distinct(name) FROM mz_objects;
    with open(MZ_ROOT / "test" / "workload-replay" / "objects.txt") as f:
        result |= set(
            line.strip().lower()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )
    return result


# Keys that are part of the workload file format itself, not user identifiers.
# The verify pass must not treat these as leaks when a user object happens to
# share a name with one of them (e.g. a column named `transaction_id`). Their
# *values* are still checked; only the structural key name is exempt.
RESERVED_FORMAT_KEYS = frozenset(
    {
        # top level
        "databases",
        "clusters",
        "queries",
        "mz_workload_version",
        # schema-level containers
        "tables",
        "views",
        "materialized_views",
        "indexes",
        "types",
        "connections",
        "sources",
        "sinks",
        # object / column fields
        "create_sql",
        "name",
        "type",
        "schema",
        "database",
        "columns",
        "managed",
        "children",
        "nullable",
        "default",
        "rows",
        "avg_size",
        # source statistics fields
        "bytes_total",
        "messages_total",
        "bytes_second",
        "messages_second",
        # query record fields
        "sql",
        "cluster",
        "search_path",
        "statement_type",
        "finished_status",
        "params",
        "transaction_isolation",
        "session_id",
        "transaction_id",
        "began_at",
        "duration",
        "result_size",
    }
)


def _iter_sql(obj: Any, path: str = "") -> Any:
    """Yield (location, sql) for every create_sql/sql string in the workload."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key}"
            if key in ("create_sql", "sql") and isinstance(value, str):
                yield child_path, value
            else:
                yield from _iter_sql(value, child_path)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            yield from _iter_sql(value, f"{path}[{i}]")


def _iter_strings(obj: Any, path: str = "") -> Any:
    """Yield (location, string) for every string in the workload, keys included.

    Identifiers leak through structural positions too — notably dict keys such
    as a source child's fully-qualified name — so the identifier check must look
    beyond create_sql/sql values.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key}"
            if isinstance(key, str):
                yield f"{child_path}.<KEY>", key
            yield from _iter_strings(value, child_path)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            yield from _iter_strings(value, f"{path}[{i}]")
    elif isinstance(obj, str):
        yield path, obj


def verify_anonymized(
    new: dict[str, Any], mapping: dict[str, str], args: argparse.Namespace
) -> list[str]:
    """Best-effort scan of anonymized output for data that should have been scrubbed.

    This is a backstop for the heuristic text substitution, not a proof: it
    catches whole-word survivals of original identifiers (in any string,
    including structural dict keys) and any single-quoted literal in SQL that
    was not reduced to a placeholder ('<REDACTED>' from the parser-based path,
    or 'literal_N' from the regex fallback). It cannot detect sensitive data
    hidden in dollar-quoted strings, comments, or numeric literals when the
    regex fallback is in use.

    Cluster create_sql is exempt from the literal check: its literals (SIZE,
    replication factor, availability zones) are non-sensitive configuration that
    replay must preserve verbatim, so they are intentionally not anonymized.
    """
    problems: list[str] = []

    # Identifiers that were actually renamed (keywords map to themselves).
    identifier_checks: list[tuple[str, re.Pattern[str]]] = []
    if args.identifiers:
        for original, anonymized in mapping.items():
            if original == anonymized:
                continue
            if re.fullmatch(r"\w+", original):
                pattern = re.compile(r"\b" + re.escape(original) + r"\b")
            else:
                pattern = re.compile(re.escape(original))
            identifier_checks.append((original, pattern))

    string_literal = re.compile(r"'(?:[^']|'')*'")
    placeholder = re.compile(r"^'(?:literal_\d+|<REDACTED>)'$")

    # The identifier check runs over identifier positions only: SQL text and
    # structural dict keys (e.g. a source child's fully-qualified key). It must
    # NOT scan arbitrary scalar values — a kept literal like 'secret note' can
    # contain a word that matches a renamed column without being a leak.
    def check_identifiers(location: str, text: str) -> None:
        for original, pattern in identifier_checks:
            if pattern.search(text):
                problems.append(
                    f"{location}: original identifier {original!r} survived"
                )

    for location, text in _iter_strings(new):
        if location.endswith(".<KEY>") and text not in RESERVED_FORMAT_KEYS:
            check_identifiers(location, text)

    for location, sql in _iter_sql(new):
        check_identifiers(location, sql)
        # Literal check runs only over SQL, and exempts cluster create_sql.
        if args.literals and not location.startswith(".clusters"):
            for match in string_literal.finditer(sql):
                if not placeholder.fullmatch(match.group(0)):
                    problems.append(
                        f"{location}: non-anonymized string literal {match.group(0)!r}"
                    )

    return problems


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mz-workload-anonymize",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Anonymize identifiers and literals in a workload capture file",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Path to write the workload.yml, or - for stdout. Required unless --in-place is given.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input file with the anonymized workload. Destroys the original capture.",
    )
    parser.add_argument(
        "--identifiers", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--literals", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--verify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After anonymizing, scan the output for surviving original identifiers and "
        "non-anonymized string literals, and refuse to write if any are found.",
    )

    parser.add_argument(
        "file",
        type=str,
        help="Input workload.yml",
    )

    args = parser.parse_args()

    with open(args.file) as f:
        workload = yaml.load(f, Loader=yaml.CSafeLoader)

    kws = keywords()

    new = {
        "databases": {},
        "clusters": {},
        "queries": [],
        "mz_workload_version": "1.0.0",
    }
    mapping: dict[str, str] = {}

    count = {
        "schemas": 0,
        "tables": 0,
        "connections": 0,
        "sources": 0,
        "children": 0,
        "views": 0,
        "mvs": 0,
        "indexes": 0,
        "sinks": 0,
        "types": 0,
        "columns": 0,
        "literals": 0,
    }

    def set_name(name: str, new_name: str) -> str:
        if args.identifiers:
            if name.lower() in kws:
                new_name = name
            mapping[name] = new_name
            return new_name
        else:
            return name

    # Matches a single-quoted SQL string literal, including '' escapes. Written
    # without nested quantifiers to avoid catastrophic backtracking (ReDoS).
    string_literal_pattern = re.compile(r"'(?:[^']|'')*'")

    def anonymize_string_literal(match: re.Match[str]) -> str:
        count["literals"] += 1
        return f"'literal_{count['literals']}'"

    def anonymize_literals_in_sql(sql: str) -> str:
        return string_literal_pattern.sub(anonymize_string_literal, sql)

    def anonymize_column_default(column: dict[str, Any]) -> None:
        """Anonymize string default values in columns."""
        default = column.get("default")
        if default is not None and default != "NULL":
            if isinstance(default, str) and default.startswith("'"):
                count["literals"] += 1
                column["default"] = f"'literal_{count['literals']}'"

    for i, (name, cluster) in enumerate(workload["clusters"].items()):
        new_name = set_name(name, f"cluster_{i}")
        new["clusters"][new_name] = cluster

    for i, (db_name, database) in enumerate(workload["databases"].items()):
        new_db_name = set_name(db_name, f"db_{i}")
        new_database = {}
        for schema_name, schema in database.items():
            count["schemas"] += 1
            new_schema_name = set_name(schema_name, f"schema_{count['schemas']}")
            new_schema = {
                "tables": {},
                "views": {},
                "materialized_views": {},
                "indexes": {},
                "types": {},
                "connections": {},
                "sources": {},
                "sinks": {},
            }

            for type_name, typ in schema["types"].items():
                count["types"] += 1
                new_type_name = set_name(type_name, f"type_{count['types']}")
                new_schema["types"][new_type_name] = typ

            for table_name, table in schema["tables"].items():
                count["tables"] += 1
                new_table_name = set_name(table_name, f"table_{count['tables']}")
                old_columns = table["columns"]
                table["columns"] = []
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    if args.literals:
                        anonymize_column_default(column)
                    table["columns"].append(column)
                new_schema["tables"][new_table_name] = table

            for conn_name, conn in schema["connections"].items():
                count["connections"] += 1
                new_conn_name = set_name(conn_name, f"conn_{count['connections']}")
                new_schema["connections"][new_conn_name] = conn

            for source_name, source in schema["sources"].items():
                count["sources"] += 1
                new_source_name = set_name(source_name, f"source_{count['sources']}")
                if "columns" in source:
                    old_columns = source["columns"]
                    source["columns"] = []
                    for column in old_columns:
                        count["columns"] += 1
                        new_column_name = set_name(
                            column["name"], f"column_{count['columns']}"
                        )
                        column["name"] = new_column_name
                        if args.literals:
                            anonymize_column_default(column)
                        source["columns"].append(column)
                if "children" in source:
                    old_children = source["children"]
                    source["children"] = {}
                    for child_name_full, child in old_children.items():
                        count["children"] += 1
                        new_child_name = set_name(
                            child["name"], f"child_{count['children']}"
                        )
                        child["name"] = new_child_name
                        old_columns = child["columns"]
                        child["columns"] = []
                        for column in old_columns:
                            count["columns"] += 1
                            new_column_name = set_name(
                                column["name"], f"column_{count['columns']}"
                            )
                            column["name"] = new_column_name
                            if args.literals:
                                anonymize_column_default(column)
                            child["columns"].append(column)
                        # Build the child's fully-qualified key from the mapped
                        # database/schema names. child["name"] is already
                        # anonymized above, but database/schema are remapped
                        # later (pass 2); without mapping them here the key
                        # leaks the original database and schema names. When
                        # --identifiers is off the mapping is empty and these
                        # resolve back to the originals, as intended.
                        source["children"][
                            f"{mapping.get(child['database'], child['database'])}."
                            f"{mapping.get(child['schema'], child['schema'])}."
                            f"{child['name']}"
                        ] = child
                new_schema["sources"][new_source_name] = source

            for view_name, view in schema["views"].items():
                count["views"] += 1
                new_view_name = set_name(view_name, f"view_{count['views']}")
                old_columns = view["columns"]
                view["columns"] = []
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    view["columns"].append(column)
                new_schema["views"][new_view_name] = view

            for mv_name, mv in schema["materialized_views"].items():
                count["mvs"] += 1
                new_mv_name = set_name(mv_name, f"mv_{count['mvs']}")
                old_columns = mv["columns"]
                mv["columns"] = []
                for column in old_columns:
                    count["columns"] += 1
                    new_column_name = set_name(
                        column["name"], f"column_{count['columns']}"
                    )
                    column["name"] = new_column_name
                    mv["columns"].append(column)
                new_schema["materialized_views"][new_mv_name] = mv

            for index_name, index in schema["indexes"].items():
                count["indexes"] += 1
                new_index_name = set_name(index_name, f"index_{count['indexes']}")
                new_schema["indexes"][new_index_name] = index

            for sink_name, sink in schema["sinks"].items():
                count["sinks"] += 1
                new_sink_name = set_name(sink_name, f"sink_{count['sinks']}")
                new_schema["sinks"][new_sink_name] = sink

            new_database[new_schema_name] = new_schema
        new["databases"][new_db_name] = new_database

    # TODO: Case discrepancies are not handled. You can call a column `mintimestamp`, but then use it as `minTimestamp`
    if args.identifiers:
        pattern = re.compile(
            "|".join(map(re.escape, sorted(mapping, key=len, reverse=True)))
        )

    def replace_identifiers(d: dict[str, Any], entry: str) -> None:
        if args.identifiers:
            d[entry] = pattern.sub(lambda m: mapping[m.group(0)], d[entry])

    # DDL create_sql is redacted with the blanket regex: option strings like
    # connection hosts, sink topics, and source options must be scrubbed, and
    # the parser's to_ast_string_redacted() intentionally does NOT redact those
    # (it only redacts expression/value literals, treating DDL options as
    # config). Query SQL is handled separately, via the parser (see below).
    def replace_literals(d: dict[str, Any], entry: str) -> None:
        if args.literals:
            d[entry] = anonymize_literals_in_sql(d[entry])

    # TODO: The create_sql replacements are more of a heuristic because we might overwrite identifiers and same name existing twice. There are two alternatives:
    # 1. Wrap the Mz parser in Python, parse the SQL, resolve what they map to, rename, and reserialize.
    # 2. Spin up Materialize, RENAME everything. Doesn't work for column names? Then take a new recording
    for cluster in new["clusters"].values():
        replace_identifiers(cluster, "create_sql")
    for db in new["databases"].values():
        for schema in db.values():
            for table in schema["tables"].values():
                for column in table["columns"]:
                    if args.identifiers and column["type"] in mapping:
                        column["type"] = mapping[column["type"]]
                replace_identifiers(table, "create_sql")
                replace_literals(table, "create_sql")
            for typ in schema["types"].values():
                replace_identifiers(typ, "create_sql")
                replace_literals(typ, "create_sql")
            for conn in schema["connections"].values():
                replace_identifiers(conn, "create_sql")
                # Connection create_sql carries hostnames, usernames, regions,
                # bucket/broker URLs, etc. as string literals; anonymize them.
                replace_literals(conn, "create_sql")
            for source in schema["sources"].values():
                for column in source.get("columns", []):
                    if args.identifiers and column["type"] in mapping:
                        column["type"] = mapping[column["type"]]
                for child in source.get("children", {}).values():
                    if args.identifiers:
                        # A child's schema/database may be a builtin or otherwise
                        # uncaptured name that never entered the mapping; leave
                        # those as-is rather than crashing.
                        child["schema"] = mapping.get(child["schema"], child["schema"])
                        child["database"] = mapping.get(
                            child["database"], child["database"]
                        )
                    for column in child["columns"]:
                        if args.identifiers and column["type"] in mapping:
                            column["type"] = mapping[column["type"]]
                    replace_identifiers(child, "create_sql")
                    replace_literals(child, "create_sql")
                replace_identifiers(source, "create_sql")
                replace_literals(source, "create_sql")
            for view in schema["views"].values():
                replace_identifiers(view, "create_sql")
                replace_literals(view, "create_sql")
            for mv in schema["materialized_views"].values():
                replace_identifiers(mv, "create_sql")
                replace_literals(mv, "create_sql")
            for index in schema["indexes"].values():
                replace_identifiers(index, "create_sql")
                replace_literals(index, "create_sql")
            for sink in schema["sinks"].values():
                replace_identifiers(sink, "create_sql")
                # Sink create_sql carries topic names, broker lists, and
                # bucket/path URLs as string literals; anonymize them.
                replace_literals(sink, "create_sql")
    query_literal_targets: list[dict[str, Any]] = []
    for query in workload["queries"]:
        if args.identifiers:
            query["cluster"] = mapping.get(query["cluster"], query["cluster"])
            query["database"] = mapping.get(query["database"], query["database"])
            query["search_path"] = [
                mapping.get(schema, schema) for schema in query["search_path"]
            ]
            replace_identifiers(query, "sql")
        if args.literals:
            query_literal_targets.append(query)
        new["queries"].append(query)

    # Redact literals in query SQL with Materialize's own parser, in one batch.
    # The parser handles every literal form the dialect supports (numbers, hex
    # strings, intervals, dollar-quoted and escape strings) where the regex only
    # caught single-quoted strings. Fall back to the regex per-statement when
    # the helper binary is unavailable or cannot parse a given statement.
    if query_literal_targets:
        sqls = [q["sql"] for q in query_literal_targets]
        redacted = redact_literals_via_parser(sqls)
        if redacted is None:
            print(
                "warning: mz-sql-anonymize helper not found; using regex literal "
                "redaction for queries, which misses numbers, dollar-quoted "
                "strings, and comments. Build it for exact redaction:\n"
                "    cargo build --release -p mz-sql-anonymize",
                file=sys.stderr,
            )
            for q in query_literal_targets:
                q["sql"] = anonymize_literals_in_sql(q["sql"])
        else:
            for q, red in zip(query_literal_targets, redacted):
                q["sql"] = (
                    red if red is not None else anonymize_literals_in_sql(q["sql"])
                )

    if args.verify:
        problems = verify_anonymized(new, mapping, args)
        if problems:
            print(
                "Refusing to write output: anonymization left sensitive data behind.\n"
                "Pass --no-verify to write anyway.",
                file=sys.stderr,
            )
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            return 1

    if args.output:
        output = args.output
    elif args.in_place:
        output = args.file
    else:
        print(
            "error: specify an output with -o/--output (use '-' for stdout) "
            "or pass --in-place to overwrite the input file",
            file=sys.stderr,
        )
        return 1

    if output == "-":
        yaml.dump(new, sys.stdout, Dumper=yaml.CSafeDumper)
    else:
        with open(output, "w") as f:
            yaml.dump(new, f, Dumper=yaml.CSafeDumper)

    return 0


if __name__ == "__main__":
    sys.exit(main())
