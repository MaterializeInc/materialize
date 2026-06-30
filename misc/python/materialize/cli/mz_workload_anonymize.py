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
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT


def _read_workload(path: str) -> dict[str, Any]:
    """Load a workload from a YAML capture file."""
    with open(path) as f:
        return yaml.load(f, Loader=yaml.CSafeLoader)


def _write_workload(workload: dict[str, Any], output: str) -> None:
    """Write a workload to `output` (`-` for stdout)."""
    if output == "-":
        yaml.dump(workload, sys.stdout, Dumper=yaml.CSafeDumper)
    else:
        with open(output, "w") as f:
            yaml.dump(workload, f, Dumper=yaml.CSafeDumper)


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


def _run_helper(request: dict[str, Any]) -> Any:
    """Run the `mz-sql-anonymize` helper with a JSON request, return its JSON.

    Callers must have ensured the helper is available (see `_locate_redactor`).
    Raises if the helper errors (e.g. a statement could not be parsed).
    """
    cmd = _locate_redactor()
    assert cmd is not None, "caller must ensure the helper is available"
    proc = subprocess.run(
        cmd, input=json.dumps(request), capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed:\n{proc.stderr}")
    return json.loads(proc.stdout)


def anonymize_sql_via_parser(
    sqls: list[str],
    mapping: dict[str, str],
    keywords: list[str],
    rename_identifiers: bool,
    redact_literals: bool,
    redact_numbers: bool,
    consistent_names: bool,
    salt: str,
) -> list[str | None]:
    """Rename identifiers and/or redact literals in each SQL string via the AST.

    Uses the `mz-sql-anonymize` helper, which parses each statement with
    Materialize's own parser, renames identifier tokens per `mapping`, and (when
    `redact_literals`) replaces literal values — including typed option strings
    like broker hosts. With `consistent_names` each such string is replaced by a
    stable, valid `redacted_<hash>` token (the same input always maps to the
    same token), so cross-references survive — e.g. a source's Kafka topic stays
    linked to the rest of its config; without it the value becomes the inert
    `'<REDACTED>'` placeholder. With `redact_numbers` it also redacts numeric
    literals to the neutral literal `1` (not a string, which would not parse
    where a number is required). `redact_numbers` is set for queries and for
    view/materialized-view/index *bodies* (predicates like `WHERE ssn = 123` are
    data), but NOT for tables/sources/sinks/clusters, whose numbers sit in option
    positions (sizes, ports, replication factors, column defaults) that are
    config replay needs valid. `salt` is a per-run random value mixed into every
    `redacted_<hash>` token so they cannot be reversed by a dictionary attack on
    the shared output; the same salt is passed for every call in one run so the
    tokens stay consistent. Doing this on the AST avoids the corruption a text
    regex causes (substring matches, in-string rewrites, broken syntax).

    Returns a list aligned with the input — the rewritten SQL, or None for a
    statement that did not parse. Raises if the helper binary errors; callers
    must have ensured it is available (see `_locate_redactor`).
    """
    return _run_helper(
        {
            "mapping": mapping,
            "keywords": keywords,
            "rename_identifiers": rename_identifiers,
            "redact_literals": redact_literals,
            "redact_numbers": redact_numbers,
            "consistent_names": consistent_names,
            "salt": salt,
            "statements": sqls,
        }
    )


def collect_object_names_via_parser(
    sqls: list[str], mapping: dict[str, str], keywords: list[str]
) -> list[str]:
    """Return object names referenced in `sqls` that are not in `mapping`.

    Catches objects created/dropped *during* the capture window (dbt-built
    views, transient deploy clusters, their schemas), which are absent from the
    catalog snapshot the mapping is built from, so the caller can assign them
    anonymized names before the rewrite. Uses the `mz-sql-anonymize` helper in
    its collect mode; see `Request.collect` there.
    """
    return _run_helper(
        {
            "mapping": mapping,
            "keywords": keywords,
            "collect": True,
            "statements": sqls,
        }
    )


# Built-in system schemas. They qualify built-in types and functions throughout
# captured DDL (e.g. a column typed `pg_catalog.uuid`, a call to
# `mz_catalog.mz_version()`), so they are not user identifiers; renaming them
# yields references to schemas that do not exist, breaking replay. Mirrors
# `SYSTEM_SCHEMAS` in src/repr/src/namespaces.rs (plus the temp schema).
SYSTEM_SCHEMAS = frozenset(
    {
        "mz_catalog",
        "mz_catalog_unstable",
        "pg_catalog",
        "mz_internal",
        "mz_introspection",
        "information_schema",
        "mz_unsafe",
        "mz_temp",
    }
)


# Mirror of `is_number_interval` in the mz-sql-anonymize helper. The helper keeps
# (does not redact) two kinds of DDL string value, so the verify pass must not
# flag them: a number-bearing duration (`'1s'`, `'5 minutes'`, `'00:05:00'`) and
# a bare datetime-field keyword (a `date_trunc`/`date_part` argument). A duration
# needs BOTH a magnitude and a unit: a bare unit word (`'y'`, `'min'`) and a bare
# number or number sequence (`'123456789'`, `'555 1234'`) are data, not
# durations, and ARE redacted/flagged.
_DATE_FIELDS = frozenset(
    {
        "microseconds", "microsecond", "milliseconds", "millisecond",
        "second", "minute", "hour", "day", "week", "month", "quarter", "year",
        "decade", "century", "millennium", "dow", "doy", "isodow", "isoyear",
        "epoch", "julian", "timezone", "timezone_hour", "timezone_minute",
    }
)  # fmt: skip
_INTERVAL_UNITS = frozenset(
    {
        "microseconds", "microsecond", "us",
        "milliseconds", "millisecond", "ms",
        "seconds", "second", "secs", "sec", "s",
        "minutes", "minute", "mins", "min", "m",
        "hours", "hour", "hrs", "hr", "h",
        "days", "day", "d", "weeks", "week", "w",
        "months", "month", "mons", "mon",
        "years", "year", "yrs", "yr", "y",
    }
)  # fmt: skip


def _is_number(token: str) -> bool:
    return (
        token != ""
        and all(c.isdigit() or c == "." for c in token)
        and any(c.isdigit() for c in token)
    )


def is_interval_like(s: str) -> bool:
    """Mirror of the helper's `is_interval_like`."""
    s = s.strip()
    if not s:
        return False
    # A bare datetime field (no magnitude) is kept for date_trunc/date_part.
    if s.lower() in _DATE_FIELDS:
        return True
    # Otherwise a duration must carry both a numeric magnitude and a unit (or a
    # colon-time word); a bare unit word or a bare number is data and gets
    # redacted.
    saw_number = False
    saw_unit = False
    for word in s.split():
        word = word.lower()
        if _is_number(word):
            saw_number = True
            continue
        if (
            ":" in word
            and all(c.isdigit() or c in ":." for c in word)
            and any(c.isdigit() for c in word)
        ):
            saw_number = True
            saw_unit = True
            continue
        if word in _INTERVAL_UNITS:
            saw_unit = True
            continue
        idx = next((i for i, c in enumerate(word) if c.isalpha()), None)
        if idx is not None:
            num, unit = word[:idx], word[idx:]
            if _is_number(num) and unit in _INTERVAL_UNITS:
                saw_number = True
                saw_unit = True
                continue
        return False
    return saw_number and saw_unit


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
    # System catalog identifiers (object/column/function/type names). A user
    # view or query over the system catalog (e.g. `SELECT password_hash FROM
    # mz_internal.mz_role_auth`) references built-in column/object names that are
    # not user identifiers; renaming them yields columns that do not exist,
    # breaking replay. See the file header for how it is generated.
    with open(
        MZ_ROOT / "test" / "workload-replay" / "system_catalog_identifiers.txt"
    ) as f:
        result |= set(
            line.strip().lower()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )
    # Built-in load-generator output columns (l_quantity, c_acctbal, ...). MZ
    # generates these itself for a load-generator source, so a view over one must
    # reference the built-in names; renaming them yields columns that do not
    # exist. See the file header for how it is generated.
    with open(
        MZ_ROOT / "test" / "workload-replay" / "load_generator_identifiers.txt"
    ) as f:
        result |= set(
            line.strip().lower()
            for line in f.readlines()
            if not line.startswith("#") and len(line.strip()) > 0
        )
    result |= SYSTEM_SCHEMAS
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


# Query statement types whose literals are non-sensitive config the anonymizer
# preserves — this MUST mirror `preserves_literals` in the mz-sql-anonymize
# helper so verify does not flag a literal the helper intentionally kept. Cluster
# DDL (sizes, replication, schedules, timeouts) is preserved too and can appear
# as a query (e.g. `CREATE CLUSTER`, `ALTER CLUSTER`) in the activity log, not
# only in the `clusters` section.
CONFIG_STATEMENT_TYPES = frozenset(
    {
        "set_variable",
        "reset_variable",
        "set_transaction",
        "alter_system_set",
        "alter_system_reset",
        "create_cluster",
        "create_cluster_replica",
        "alter_cluster",
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

    This is a backstop, not a proof: it catches whole-word survivals of original
    identifiers (in any string, including structural dict keys) and any
    single-quoted literal in SQL that was not reduced to the `'<REDACTED>'`
    placeholder. It cannot detect sensitive data hidden in dollar-quoted strings
    or comments.

    Cluster create_sql is exempt from the literal check: its literals (SIZE,
    replication factor, availability zones) are non-sensitive configuration that
    replay must preserve verbatim, so they are intentionally not anonymized.
    """
    problems: list[str] = []

    # Identifiers that were actually renamed (keywords map to themselves). Split
    # into whole-word identifiers — the vast majority — and the rare ones with
    # non-word characters. A word identifier survives as a `\b<id>\b` match iff
    # it appears as a whole `\w+` token, so we tokenize each string once and
    # intersect with this set: O(text) per string instead of O(mapping). At
    # ~10^5 queries × ~10^4 renames the old per-identifier regex scan was
    # billions of searches (minutes of CPU); this makes it linear.
    word_originals: set[str] = set()
    other_checks: list[tuple[str, re.Pattern[str]]] = []
    if args.identifiers:
        for original, anonymized in mapping.items():
            if original == anonymized:
                continue
            if re.fullmatch(r"\w+", original):
                word_originals.add(original)
            else:
                other_checks.append((original, re.compile(re.escape(original))))

    word_token = re.compile(r"\w+")
    string_literal = re.compile(r"'(?:[^']|'')*'")
    # An anonymized string literal is one of:
    #  - the inert `<REDACTED>` placeholder (query data literals),
    #  - a stable `redacted_<hash>` token from consistent renaming (DDL data:
    #    Kafka topics, hosts, external references),
    #  - a query-local `local_<n>` name (a CTE/alias referenced as a string).
    placeholder = re.compile(r"^'(?:<REDACTED>|redacted_[0-9a-f]+|local_[0-9]+)'$")
    # A renamed catalog/collected object can legitimately appear as a string
    # literal (e.g. a value referencing an object by name, or an upstream
    # database that also shows up as an external reference); the anonymizer
    # rewrote it to its anonymized name, which is safe. Identity entries (kept
    # keywords/builtins mapped to themselves) are NOT renames, so excluding them
    # keeps a data literal that merely spells a keyword (`'id'`, `'text'`) flagged.
    anon_values = {anon for orig, anon in mapping.items() if orig != anon}

    def is_anonymized_literal(literal: str) -> bool:
        # `literal` includes the surrounding quotes; `content` is the value, with
        # SQL `''` un-escaped to a single quote.
        content = literal[1:-1].replace("''", "'")
        return (
            placeholder.fullmatch(literal) is not None
            or content in anon_values
            # Durations/intervals are non-sensitive config the helper keeps.
            or is_interval_like(content)
            # Inline Avro/Protobuf schemas (typed `String` fields) are kept
            # verbatim so replay can decode against them; they are JSON.
            or content.lstrip().startswith(("{", "["))
        )

    # `MAP[...]` options (e.g. `TOPIC CONFIG = MAP['retention.ms' => '...']`) are
    # non-sensitive Kafka/topic config the anonymizer preserves verbatim, so
    # their key/value string literals must not be flagged.
    config_map = re.compile(r"MAP\[.*?\]", re.DOTALL)
    # Vetted preserve-list options whose value the helper keeps verbatim (see
    # `visit_kafka_sink_config_option_mut`): a fixed-enum config the replayed DDL
    # needs valid. Exempt the value literal in the same option position, mirroring
    # the helper precisely rather than blanket-accepting the enum word anywhere.
    preserved_option = re.compile(
        r"COMPRESSION TYPE\s*=?\s*'(?:[^']|'')*'", re.IGNORECASE
    )
    # `mz_load_generators.<gen>.<table>` references built-in load-generator
    # outputs; the helper keeps the whole reference, so its components (which can
    # be user-identifier words like `accounts`/`orders`) are not leaks.
    load_generator_ref = re.compile(r"mz_load_generators(?:\.\w+)+")
    # An `INTERVAL '…'` literal (a `Value::Interval`) keeps its value part
    # verbatim (a duration magnitude, e.g. `INTERVAL '60' DAY`). It surfaces as a
    # bare-number string literal but is config, not data, so exempt the value
    # literal in THAT position only, not bare numbers anywhere, which would mask
    # a leaked numeric-string predicate (an SSN/account id in a view body).
    interval_value = re.compile(r"\bINTERVAL\s+('(?:[^']|'')*')", re.IGNORECASE)

    # The identifier check runs over identifier positions only: SQL text and
    # structural dict keys (e.g. a source child's fully-qualified key). It must
    # NOT scan arbitrary scalar values — a kept literal like 'secret note' can
    # contain a word that matches a renamed column without being a leak.
    def check_identifiers(location: str, text: str) -> None:
        if word_originals:
            for token in set(word_token.findall(text)):
                if token in word_originals:
                    problems.append(
                        f"{location}: original identifier {token!r} survived"
                    )
        for original, pattern in other_checks:
            if pattern.search(text):
                problems.append(
                    f"{location}: original identifier {original!r} survived"
                )

    for location, text in _iter_strings(new):
        if location.endswith(".<KEY>") and text not in RESERVED_FORMAT_KEYS:
            check_identifiers(location, text)

    query_location = re.compile(r"\.queries\[(\d+)\]\.sql$")

    def literals_preserved(location: str) -> bool:
        # Cluster create_sql and config statements (SET/RESET/ALTER SYSTEM) keep
        # their literals on purpose; the anonymizer does not redact them, so the
        # verify pass must not flag them.
        if location.startswith(".clusters"):
            return True
        m = query_location.match(location)
        if (
            m
            and new["queries"][int(m.group(1))].get("statement_type")
            in CONFIG_STATEMENT_TYPES
        ):
            return True
        return False

    for location, sql in _iter_sql(new):
        # Scan for surviving identifiers outside string literals only: a word
        # inside a preserved literal (e.g. 'secret note' under --no-literals) is
        # data, not an identifier reference. Renaming runs on the AST and never
        # rewrites inside strings, so a real identifier leak shows up as a token.
        # Also blank kept load-generator references, whose built-in component
        # names are not user-identifier leaks.
        scannable = load_generator_ref.sub(" ", string_literal.sub("''", sql))
        check_identifiers(location, scannable)
        if args.literals and not literals_preserved(location):
            exempt_spans = [m.span() for m in config_map.finditer(sql)]
            exempt_spans += [m.span() for m in preserved_option.finditer(sql)]
            exempt_spans += [m.span(1) for m in interval_value.finditer(sql)]
            for match in string_literal.finditer(sql):
                if any(start <= match.start() < end for start, end in exempt_spans):
                    continue  # preserved config map entry or vetted option value
                if not is_anonymized_literal(match.group(0)):
                    problems.append(
                        f"{location}: non-anonymized string literal {match.group(0)!r}"
                    )

    # Bound parameters are redacted to the '<REDACTED>' placeholder (or kept as
    # null); anything else is un-anonymized data.
    if args.literals:
        for i, query in enumerate(new.get("queries", [])):
            params = query.get("params")
            if isinstance(params, list):
                for j, param in enumerate(params):
                    if param is not None and param != "<REDACTED>":
                        problems.append(
                            f".queries[{i}].params[{j}]: non-anonymized bound parameter {param!r}"
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

    # Resolve the output target up front so an invalid invocation fails before
    # any work (and before the parser-availability check below).
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

    workload = _read_workload(args.file)

    kws = keywords()
    # Passed to the helper so a query-local identifier (CTE/alias/constraint
    # name) that shadows a keyword or builtin object/function/type is not renamed.
    kw_list = sorted(kws)

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
    }

    def set_name(name: str, new_name: str) -> str:
        if args.identifiers:
            # Rename consistently: the same original name always maps to the same
            # anonymized name. The mapping is keyed by bare name and the AST
            # rewriter renames every matching token identically, so a name seen
            # more than once (e.g. a column name shared across the identically
            # structured tables of a multi-tenant source) must reuse its existing
            # entry. Overwriting it would desync the structural copy (set here,
            # per occurrence) from the SQL the rewriter emits (which uses the
            # final mapping) — e.g. a source's TEXT COLUMNS would name a column
            # the subsource it belongs to no longer has, breaking replay.
            if name in mapping:
                return mapping[name]
            if name.lower() in kws:
                new_name = name
            mapping[name] = new_name
            return new_name
        else:
            return name

    def anonymize_column_default(column: dict[str, Any]) -> None:
        """Redact string default values in columns.

        Defaults live as a structural field, not SQL, so the parser never sees
        them; redact string ones here to the `'<REDACTED>'` placeholder. Numeric,
        boolean, and function defaults (`5`, `true`, `now()`) have no string
        literal and are kept as config (mirroring the AST's DDL redaction).

        This is belt-and-suspenders: the column's `create_sql` is the
        authoritative copy and is AST-redacted *and* verified; this structural
        field only feeds replay's upstream-table DDL. So it errs conservative —
        any string-literal rendering (`'x'`, `E'x'`, `B'x'`, `U&'x'`, `$$x$$`,
        `'x'::type`) is redacted, even if that occasionally over-redacts.
        """
        default = column.get("default")
        if (
            isinstance(default, str)
            and default != "NULL"
            and re.match(r"[a-zA-Z]*&?'|\$", default)
        ):
            column["default"] = "'<REDACTED>'"

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

    # --- Pass 2: rewrite SQL text ---
    #
    # All SQL — cluster/DDL create_sql and query sql — is rewritten on the AST by
    # the mz-sql-anonymize helper, which renames whole identifier tokens and
    # redacts literal values (including typed option strings like broker hosts)
    # without the corruption a text regex causes. Cluster/SET config literals
    # (sizes, timeouts) are preserved by the helper. The parser is mandatory: if
    # the helper is missing or a statement does not parse, anonymization cannot
    # be done safely and the tool refuses to write rather than leak.

    if _locate_redactor() is None:
        print(
            "error: mz-sql-anonymize helper not found, so SQL cannot be "
            "anonymized. Build it with:\n"
            "    cargo build --release -p mz-sql-anonymize",
            file=sys.stderr,
        )
        return 1

    # Augment the mapping with objects referenced in SQL but absent from the
    # catalog snapshot — created/dropped during the capture window (dbt-built
    # views, transient deploy clusters, their schemas). They get `object_N`
    # names so they are anonymized everywhere the rewrite and the structural
    # remap below run (SQL, query routing fields, SET cluster/database literals).
    if args.identifiers:
        all_sqls = [sql for _location, sql in _iter_sql(new)]
        all_sqls += [
            q["sql"] for q in workload["queries"] if isinstance(q.get("sql"), str)
        ]
        for i, name in enumerate(
            collect_object_names_via_parser(all_sqls, mapping, kw_list), start=1
        ):
            mapping[name] = f"object_{i}"

    # Structural identifier fields that are not SQL text (the helper never sees
    # them): column type references, child schema/database, and query routing.
    if args.identifiers:
        for db in new["databases"].values():
            for schema in db.values():
                for table in schema["tables"].values():
                    for column in table["columns"]:
                        if column["type"] in mapping:
                            column["type"] = mapping[column["type"]]
                for source in schema["sources"].values():
                    for column in source.get("columns", []):
                        if column["type"] in mapping:
                            column["type"] = mapping[column["type"]]
                    # Rebuild the children dict here, not in pass 1: a child's
                    # fully-qualified key embeds its database/schema, which may be
                    # mapped only later in pass 1 (different schema, processed
                    # after this source). Built early, the key leaks the original
                    # names; rebuilt now, the mapping is complete.
                    children = source.get("children")
                    if children is not None:
                        rebuilt: dict[str, Any] = {}
                        for child in children.values():
                            # A child's schema/database may be a builtin or
                            # otherwise uncaptured name not in the mapping; leave
                            # those as-is.
                            child["schema"] = mapping.get(
                                child["schema"], child["schema"]
                            )
                            child["database"] = mapping.get(
                                child["database"], child["database"]
                            )
                            for column in child["columns"]:
                                if column["type"] in mapping:
                                    column["type"] = mapping[column["type"]]
                            rebuilt[
                                f"{child['database']}.{child['schema']}.{child['name']}"
                            ] = child
                        source["children"] = rebuilt
        for query in workload["queries"]:
            query["cluster"] = mapping.get(query["cluster"], query["cluster"])
            query["database"] = mapping.get(query["database"], query["database"])
            query["search_path"] = [
                mapping.get(schema, schema) for schema in query["search_path"]
            ]

    # Bound query parameters (the `$1, $2, ...` a prepared statement was executed
    # with) are user data values living as a structural list, not SQL, so the AST
    # never sees them. Redact each to the same placeholder; replay maps it back
    # to NULL (see pg_params_to_psycopg), as it does for redacted SQL literals.
    if args.literals:
        for query in workload["queries"]:
            params = query.get("params")
            if isinstance(params, list):
                query["params"] = [None if p is None else "<REDACTED>" for p in params]

    for query in workload["queries"]:
        new["queries"].append(query)

    # Rewrite each group of create_sql/sql strings through the helper. A
    # per-run random salt makes the `redacted_<hash>` tokens unguessable from the
    # shared output while staying consistent across these calls. A statement that
    # fails to parse is collected for the hard-error check below.
    salt = secrets.token_hex(16)
    unparsed: list[str] = []

    def anonymize_group(
        items: list[dict[str, Any]],
        key: str,
        *,
        redact_numbers: bool,
        consistent_names: bool,
    ) -> None:
        targets = [d for d in items if isinstance(d.get(key), str)]
        if not targets:
            return
        sqls = [d[key] for d in targets]
        results = anonymize_sql_via_parser(
            sqls,
            mapping,
            kw_list,
            args.identifiers,
            args.literals,
            redact_numbers,
            consistent_names,
            salt,
        )
        for d, out in zip(targets, results):
            if out is None:
                unparsed.append(d[key])
            else:
                d[key] = out

    # All DDL uses consistent renaming so a redacted value (Kafka topic, external
    # reference, connection host) maps to a stable valid token and stays linked
    # across the config replay depends on. They split on numbers, though:
    #  - View/MV/index *bodies* are queries; a number there (`WHERE ssn = 123`)
    #    is data and is redacted, like the queries group.
    #  - Tables/sources/sinks/clusters/types keep numbers: theirs sit in option
    #    positions (sizes, ports, replication factors, column defaults) that are
    #    config replay needs valid.
    # Queries use the inert `<REDACTED>` placeholder for strings (no
    # cross-statement linkage to preserve) and also redact numbers.
    config_ddl: list[dict[str, Any]] = list(new["clusters"].values())
    body_ddl: list[dict[str, Any]] = []
    for db in new["databases"].values():
        for schema in db.values():
            for group in (
                "tables",
                "types",
                "connections",
                "sources",
                "views",
                "materialized_views",
                "indexes",
                "sinks",
            ):
                target = (
                    body_ddl
                    if group in ("views", "materialized_views", "indexes")
                    else config_ddl
                )
                for obj in schema[group].values():
                    target.append(obj)
                    if group == "sources":
                        config_ddl.extend(obj.get("children", {}).values())

    anonymize_group(
        config_ddl, "create_sql", redact_numbers=False, consistent_names=True
    )
    anonymize_group(body_ddl, "create_sql", redact_numbers=True, consistent_names=True)
    anonymize_group(new["queries"], "sql", redact_numbers=True, consistent_names=False)

    if unparsed:
        print(
            f"error: mz-sql-anonymize could not parse {len(unparsed)} statement(s); "
            "they cannot be anonymized safely, so refusing to write. This likely "
            "indicates an unsupported statement or a parser bug — please report it.",
            file=sys.stderr,
        )
        return 1

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

    _write_workload(new, output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
