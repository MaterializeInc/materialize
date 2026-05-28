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
) -> tuple[int, dict[str, Any] | None, str]:
    """Run main() against a workload, returning (exit_code, output, dumped_text)."""
    inp = tmp_path / "workload.yml"
    inp.write_text(yaml.safe_dump(workload))
    argv = ["mz-workload-anonymize", str(inp)]
    if in_place:
        argv.append("--in-place")
        out = inp
    else:
        out = tmp_path / "out.yml"
        argv += ["-o", str(out)]
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


def test_redact_via_parser_returns_none_without_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mz_workload_anonymize, "_locate_redactor", lambda: None)
    assert mz_workload_anonymize.redact_literals_via_parser(["SELECT 1"]) is None


def test_regex_fallback_warns(
    tmp_path: Any, force_regex: None, capsys: pytest.CaptureFixture[str]
) -> None:
    rc, _out, _text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "mz-sql-anonymize helper not found" in capsys.readouterr().err


@pytest.mark.skipif(
    mz_workload_anonymize._locate_redactor() is None,
    reason="mz-sql-anonymize binary not built; run `cargo build --release -p mz-sql-anonymize`",
)
def test_parser_path_redacts_numeric_literal(tmp_path: Any) -> None:
    # The parser path (unlike the regex) redacts numbers in query predicates.
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "987654321" not in text
    assert "<REDACTED>" in text
