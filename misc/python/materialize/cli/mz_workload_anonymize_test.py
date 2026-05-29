# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for the workload anonymizer (`mz-workload-anonymize`).

These exercise the pure-Python anonymization logic and the regex-based literal
fallback, which run without a built `mz-sql-anonymize` binary. One test
covers the parser-backed path and is skipped when that binary is absent.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest import mock

import pytest
import yaml

from materialize.cli import mz_workload_anonymize


def base_workload() -> dict[str, Any]:
    """A small but structurally complete workload capture.

    Carries sensitive identifiers (database/table/column/connection/sink names)
    and literals (a connection host and user, a sink topic, a column default,
    and string + numeric predicates in a query) so tests can assert they are
    scrubbed.
    """
    return {
        "clusters": {
            "prod_cluster": {
                "create_sql": "CREATE CLUSTER prod_cluster (SIZE = '100cc')",
            },
        },
        "databases": {
            "customers_db": {
                "public": {
                    "tables": {
                        "orders": {
                            "create_sql": "CREATE TABLE orders (id int, note text DEFAULT 'secret note')",
                            "columns": [
                                {"name": "id", "type": "int4"},
                                {
                                    "name": "note",
                                    "type": "text",
                                    "default": "'secret note'",
                                },
                            ],
                            "rows": 5,
                        },
                    },
                    "views": {},
                    "materialized_views": {},
                    "indexes": {},
                    "types": {},
                    "connections": {
                        "kafka_conn": {
                            "create_sql": "CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'prod.internal.acme.com:9092', SASL USERNAME 'admin')",
                        },
                    },
                    "sources": {},
                    "sinks": {
                        "out_sink": {
                            "create_sql": "CREATE SINK out_sink FROM orders INTO KAFKA CONNECTION kafka_conn (TOPIC 'customer-orders-prod')",
                        },
                    },
                },
            },
        },
        "queries": [
            {
                "sql": "SELECT id, note FROM customers_db.public.orders WHERE note = 'hunter2' AND id = 987654321",
                "cluster": "prod_cluster",
                "database": "customers_db",
                "search_path": ["public"],
                "statement_type": "select",
                "finished_status": "success",
            },
        ],
    }


def run_tool(
    tmp_path: Any,
    workload: dict[str, Any],
    *extra_args: str,
    in_place: bool = False,
    require_parser: bool = False,
) -> tuple[int, dict[str, Any] | None, str]:
    """Run main() against a workload, returning (exit_code, output, dumped_text).

    Defaults to --no-require-parser so tests are deterministic whether or not
    the mz-sql-anonymize binary is built; tests that exercise the parser
    requirement pass require_parser=True.
    """
    inp = tmp_path / "workload.yml"
    inp.write_text(yaml.safe_dump(workload))
    argv = ["mz-workload-anonymize", str(inp)]
    if in_place:
        argv.append("--in-place")
        out = inp
    else:
        out = tmp_path / "out.yml"
        argv += ["-o", str(out)]
    if not require_parser:
        argv.append("--no-require-parser")
    argv += list(extra_args)

    with mock.patch.object(sys, "argv", argv):
        rc = mz_workload_anonymize.main()

    if rc == 0 and out.exists():
        text = out.read_text()
        return rc, yaml.safe_load(text), text
    return rc, None, ""


@pytest.fixture
def force_regex(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the regex literal fallback by hiding the parser binary.

    Keeps the bulk of the tests deterministic regardless of whether the
    `mz-sql-anonymize` helper happens to be built in the dev environment.
    """
    monkeypatch.setattr(mz_workload_anonymize, "_locate_redactor", lambda: None)


def test_anonymizes_identifiers(tmp_path: Any, force_regex: None) -> None:
    rc, out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert out is not None
    # Original object names must not survive anywhere in the output.
    for original in ("customers_db", "orders", "kafka_conn", "out_sink"):
        assert original not in text, f"{original!r} leaked"
    # And anonymized names should be present.
    assert "db_0" in text
    assert "table_1" in text


def test_connection_and_sink_literals_scrubbed(
    tmp_path: Any, force_regex: None
) -> None:
    # Regression test for the connection/sink literal leak: hostnames,
    # usernames, and topic names live in DDL option strings.
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "prod.internal.acme.com" not in text
    assert "admin" not in text
    assert "customer-orders-prod" not in text


def test_table_default_literal_scrubbed(tmp_path: Any, force_regex: None) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "secret note" not in text


def test_query_string_literal_scrubbed(tmp_path: Any, force_regex: None) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "hunter2" not in text


def test_cluster_size_preserved(tmp_path: Any, force_regex: None) -> None:
    # Cluster SIZE is non-sensitive config that replay must keep verbatim.
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "100cc" in text


def test_no_output_target_errors(
    tmp_path: Any, force_regex: None, capsys: pytest.CaptureFixture[str]
) -> None:
    inp = tmp_path / "workload.yml"
    inp.write_text(yaml.safe_dump(base_workload()))
    with mock.patch.object(sys, "argv", ["mz-workload-anonymize", str(inp)]):
        rc = mz_workload_anonymize.main()
    assert rc == 1
    assert "in-place" in capsys.readouterr().err


def test_in_place_overwrites_input(tmp_path: Any, force_regex: None) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload(), in_place=True)
    assert rc == 0
    assert "customers_db" not in text
    assert "hunter2" not in text


def test_no_literals_keeps_literals_but_anonymizes_identifiers(
    tmp_path: Any, force_regex: None
) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload(), "--no-literals")
    assert rc == 0
    # Literals retained...
    assert "hunter2" in text
    # ...but identifiers still anonymized.
    assert "customers_db" not in text


def test_verify_catches_surviving_identifier() -> None:
    new = {
        "databases": {},
        "clusters": {},
        "queries": [{"sql": "SELECT * FROM orders"}],
    }
    mapping = {"orders": "table_1"}
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, mapping, args)
    assert any("orders" in p for p in problems)


def test_verify_catches_unanonymized_literal() -> None:
    new = {
        "databases": {},
        "clusters": {},
        "queries": [{"sql": "SELECT * FROM t WHERE x = 'leak'"}],
    }
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, {}, args)
    assert any("leak" in p for p in problems)


def test_verify_accepts_both_placeholder_styles() -> None:
    new = {
        "databases": {},
        "clusters": {},
        "queries": [
            {"sql": "SELECT * FROM t WHERE a = 'literal_1' AND b = '<REDACTED>'"},
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_exempts_config_statement_literals() -> None:
    # A preserved SET/timeout literal in a config query must not be flagged.
    new = {
        "clusters": {},
        "databases": {},
        "queries": [
            {"sql": "SET statement_timeout = '5s'", "statement_type": "set_variable"},
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_exempts_cluster_literals() -> None:
    # Cluster create_sql keeps its SIZE literal; verify must not flag it.
    new = {
        "clusters": {
            "cluster_0": {"create_sql": "CREATE CLUSTER cluster_0 (SIZE = '100cc')"}
        },
        "databases": {},
        "queries": [],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_anonymize_via_parser_returns_none_without_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mz_workload_anonymize, "_locate_redactor", lambda: None)
    assert (
        mz_workload_anonymize.anonymize_sql_via_parser(["SELECT 1"], {}, True, True)
        is None
    )


def test_regex_fallback_warns(
    tmp_path: Any, force_regex: None, capsys: pytest.CaptureFixture[str]
) -> None:
    rc, _out, _text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "mz-sql-anonymize helper not found" in capsys.readouterr().err


def test_require_parser_errors_without_binary(
    tmp_path: Any, force_regex: None, capsys: pytest.CaptureFixture[str]
) -> None:
    # By default the parser is required: with no binary the tool must refuse to
    # run rather than silently fall back to the weaker regex.
    rc, out, _text = run_tool(tmp_path, base_workload(), require_parser=True)
    assert rc == 1
    assert out is None
    err = capsys.readouterr().err
    assert "mz-sql-anonymize" in err
    assert "--no-require-parser" in err


@pytest.mark.skipif(
    mz_workload_anonymize._locate_redactor() is None,
    reason="mz-sql-anonymize binary not built; run `cargo build --release -p mz-sql-anonymize`",
)
def test_parser_path_redacts_numeric_literal(tmp_path: Any) -> None:
    # The parser path (unlike the regex) redacts numbers in query predicates.
    rc, _out, text = run_tool(tmp_path, base_workload(), require_parser=True)
    assert rc == 0
    assert "987654321" not in text
    assert "<REDACTED>" in text


@pytest.mark.skipif(
    mz_workload_anonymize._locate_redactor() is None,
    reason="mz-sql-anonymize binary not built; run `cargo build --release -p mz-sql-anonymize`",
)
def test_parser_path_renames_table_reference(tmp_path: Any) -> None:
    # End-to-end proof the AST path renames a custom object reference as a whole
    # token (where the old text regex risked substring corruption). `zorp` and
    # `zorpcol` are non-keyword names, so both must be renamed and neither may
    # survive as a bare word.
    import re as _re

    wl = base_workload()
    wl["databases"]["customers_db"]["public"]["tables"]["zorp"] = {
        "create_sql": "CREATE TABLE zorp (zorpcol int)",
        "columns": [{"name": "zorpcol", "type": "int4"}],
        "rows": 1,
    }
    wl["queries"][0]["sql"] = "SELECT zorpcol FROM zorp"
    rc, out, text = run_tool(tmp_path, wl, require_parser=True)
    assert rc == 0
    assert out is not None
    assert not _re.search(r"(?<![A-Za-z0-9_])zorp(?![A-Za-z0-9_])", text), text
    assert not _re.search(r"(?<![A-Za-z0-9_])zorpcol(?![A-Za-z0-9_])", text), text


def cdc_workload() -> dict[str, Any]:
    """A workload with a CDC source whose subsources (children) are keyed by
    their fully-qualified `database.schema.name`."""
    return {
        "clusters": {},
        "databases": {
            "upstream_db": {
                "ingest_schema": {
                    "tables": {},
                    "views": {},
                    "materialized_views": {},
                    "indexes": {},
                    "types": {},
                    "connections": {},
                    "sources": {
                        "pg_src": {
                            "create_sql": "CREATE SOURCE pg_src FROM POSTGRES CONNECTION c",
                            "type": "postgres",
                            "children": {
                                "upstream_db.ingest_schema.people": {
                                    "name": "people",
                                    "database": "upstream_db",
                                    "schema": "ingest_schema",
                                    "create_sql": "CREATE SUBSOURCE people (id int)",
                                    "columns": [{"name": "id", "type": "int4"}],
                                },
                            },
                        },
                    },
                    "sinks": {},
                },
            },
        },
        "queries": [],
    }


def test_subsource_child_key_is_anonymized(tmp_path: Any, force_regex: None) -> None:
    # Regression: the child's fully-qualified dict key must not leak the
    # original database/schema/name.
    rc, _out, text = run_tool(tmp_path, cdc_workload())
    assert rc == 0
    for original in ("upstream_db", "ingest_schema", "people"):
        assert original not in text, f"{original!r} leaked via a child key"


def test_verify_catches_leaked_child_key() -> None:
    # A surviving original identifier in a structural dict key must be flagged.
    new = {
        "clusters": {},
        "databases": {
            "db_0": {
                "schema_1": {
                    "sources": {
                        "source_1": {
                            "children": {
                                # Leaky: original schema name survived in the key.
                                "db_0.ingest_schema.child_1": {"name": "child_1"},
                            },
                        },
                    },
                },
            },
        },
        "queries": [],
    }
    mapping = {"ingest_schema": "schema_1"}
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, mapping, args)
    assert any("ingest_schema" in p for p in problems)


def test_verify_ignores_reserved_format_key_collision() -> None:
    # A user column named like a reserved format key (e.g. transaction_id) must
    # not make verify flag the query record's own field name.
    new = {
        "clusters": {},
        "databases": {},
        "queries": [{"sql": "SELECT column_1 FROM table_1", "transaction_id": 7}],
    }
    mapping = {"transaction_id": "column_1"}
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, mapping, args) == []


def test_verify_ignores_identifier_word_in_scalar_value() -> None:
    # A scalar value (here a column default) may contain a word matching a
    # renamed column; that is data, not an identifier leak, so the identifier
    # check must not scan scalar values. (SQL text is scanned separately.)
    new = {
        "clusters": {},
        "databases": {
            "db_0": {
                "public": {
                    "tables": {
                        "table_1": {
                            "create_sql": "CREATE TABLE table_1 (column_1 text)",
                            "columns": [
                                {
                                    "name": "column_1",
                                    "type": "text",
                                    "default": "'secret note'",
                                }
                            ],
                        }
                    },
                }
            }
        },
        "queries": [],
    }
    mapping = {"note": "column_1"}
    args = mock.Mock(identifiers=True, literals=False)
    assert mz_workload_anonymize.verify_anonymized(new, mapping, args) == []
