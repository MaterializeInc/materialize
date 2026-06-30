# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for the workload anonymizer (`mz-workload-anonymize`).

SQL rewriting is done on the AST by the `mz-sql-anonymize` helper binary. The
`build_helper` session fixture builds it so these tests always run — they are
never silently skipped when the binary is missing. The `verify_anonymized`
backstop is pure Python and is tested directly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any
from unittest import mock

import pytest
import yaml

from materialize import MZ_ROOT
from materialize.cli import mz_workload_anonymize


@pytest.fixture(scope="session", autouse=True)
def build_helper() -> None:
    """Build the `mz-sql-anonymize` helper so the end-to-end tests always run.

    These tests drive the helper binary. Rather than skip when it is missing —
    which silently hides regressions — build it as part of running the tests, and
    fail loudly if the build itself fails. The locator is pointed at this freshly
    built binary so the tests never run against a stale one.

    The path is read from cargo's own JSON output, not assumed to be
    `target/debug/`, so it is correct even when CARGO_TARGET_DIR redirects the
    build elsewhere (as CI does).
    """
    proc = subprocess.run(
        ["cargo", "build", "-p", "mz-sql-anonymize", "--message-format=json"],
        cwd=MZ_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    executable = None
    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            msg.get("reason") == "compiler-artifact"
            and msg.get("target", {}).get("name") == "mz-sql-anonymize"
            and msg.get("executable")
        ):
            executable = msg["executable"]
    assert executable is not None, "cargo did not report the mz-sql-anonymize binary"
    os.environ["MZ_SQL_ANONYMIZE_BIN"] = executable


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


def test_anonymizes_identifiers(tmp_path: Any) -> None:
    rc, out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert out is not None
    # Original object names must not survive anywhere in the output.
    for original in ("customers_db", "orders", "kafka_conn", "out_sink"):
        assert original not in text, f"{original!r} leaked"
    # And anonymized names should be present.
    assert "db_0" in text
    assert "table_1" in text


def test_connection_and_sink_literals_scrubbed(tmp_path: Any) -> None:
    # Regression test for the connection/sink literal leak: hostnames live in a
    # typed AST field (KafkaBroker.address), while usernames and topic names are
    # option values. All must be redacted on the AST.
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "prod.internal.acme.com" not in text
    assert "admin" not in text
    assert "customer-orders-prod" not in text


def test_table_default_literal_scrubbed(tmp_path: Any) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "secret note" not in text


def test_query_string_literal_scrubbed(tmp_path: Any) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "hunter2" not in text


def test_query_numeric_literal_scrubbed(tmp_path: Any) -> None:
    # A number in a query predicate is a data value and must be redacted — to the
    # neutral literal `1` (not a string, which would not parse where a number is
    # required, e.g. LIMIT/an int comparison).
    rc, out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert out is not None
    assert "987654321" not in text
    assert "id = 1" in out["queries"][0]["sql"]


def test_view_body_numbers_redacted_but_config_numbers_kept(tmp_path: Any) -> None:
    # A number in a view/MV body is a predicate value (data) and must be
    # redacted, while a numeric column default and a cluster SIZE are config that
    # must stay valid for replay. This is the body-vs-option distinction.
    wl = base_workload()
    public = wl["databases"]["customers_db"]["public"]
    public["tables"]["orders"][
        "create_sql"
    ] = "CREATE TABLE orders (id int, qty int DEFAULT 42)"
    public["views"] = {
        "leak_view": {
            "create_sql": "CREATE VIEW leak_view AS SELECT id FROM orders WHERE ssn = 123456789 AND balance > 50000",
            "columns": [{"name": "id", "type": "int4"}],
        }
    }
    rc, out, text = run_tool(tmp_path, wl)
    assert rc == 0
    assert out is not None
    # View-body data numbers are gone.
    assert "123456789" not in text, text
    assert "50000" not in text, text
    # Config numbers survive: a numeric column default and the cluster SIZE.
    assert "DEFAULT 42" in text, "numeric column default must be kept"
    assert "100cc" in text, "cluster size must be kept"


def test_view_body_numeric_string_literal_redacted(tmp_path: Any) -> None:
    # Regression (Finding 1): a QUOTED numeric-string predicate in a view body is
    # data (an SSN, card or account number), not a bare-number "interval", and
    # must be redacted. The body path uses consistent_names AND redact_numbers,
    # which previously kept such strings verbatim; the verify pass also wrongly
    # exempted them.
    wl = base_workload()
    public = wl["databases"]["customers_db"]["public"]
    public["views"] = {
        "leak_view": {
            "create_sql": (
                "CREATE VIEW leak_view AS SELECT id FROM orders "
                "WHERE ssn = '123456789' AND cc = '4111 1111 1111 1111'"
            ),
            "columns": [{"name": "id", "type": "int4"}],
        }
    }
    rc, _out, text = run_tool(tmp_path, wl)
    assert rc == 0, "verify must not exempt the numeric-string data and must pass"
    assert "123456789" not in text, text
    assert "4111 1111 1111 1111" not in text, text


def test_materialized_view_retain_history_preserved(tmp_path: Any) -> None:
    # The flip side of the fix: an MV's RETAIN HISTORY duration is a Value::String
    # config value (it carries a unit) replay needs intact. The body path keeps
    # it despite also redacting numbers, so the numeric-string tightening must not
    # touch it. (This is why bodies cannot simply be anonymized in query mode.)
    wl = base_workload()
    public = wl["databases"]["customers_db"]["public"]
    public["materialized_views"] = {
        "mv": {
            "create_sql": (
                "CREATE MATERIALIZED VIEW mv WITH (RETAIN HISTORY = FOR '7 days') "
                "AS SELECT id FROM orders"
            ),
            "columns": [{"name": "id", "type": "int4"}],
        }
    }
    rc, out, _text = run_tool(tmp_path, wl)
    assert rc == 0
    assert out is not None
    (mv,) = out["databases"]["db_0"]["public"]["materialized_views"].values()
    assert (
        "'7 days'" in mv["create_sql"]
    ), f"retain-history duration must be preserved for replay: {mv['create_sql']}"


def _query_with_params(params: list[Any]) -> dict[str, Any]:
    return {
        "sql": "SELECT * FROM customers_db.public.orders WHERE id = $1 AND note = $2 AND x = $3",
        "cluster": "prod_cluster",
        "database": "customers_db",
        "search_path": ["public"],
        "statement_type": "select",
        "finished_status": "success",
        "params": params,
    }


def test_bound_query_params_scrubbed(tmp_path: Any) -> None:
    # Bound parameters carry user data outside the SQL text ($1, $2, ...); each
    # must be redacted to '<REDACTED>', while a null parameter stays null.
    wl = base_workload()
    wl["queries"].append(_query_with_params(["555123456", "topsecret", None]))
    rc, out, text = run_tool(tmp_path, wl)
    assert rc == 0
    assert out is not None
    assert "555123456" not in text
    assert "topsecret" not in text
    assert out["queries"][-1]["params"] == ["<REDACTED>", "<REDACTED>", None]


def test_no_literals_keeps_bound_params(tmp_path: Any) -> None:
    wl = base_workload()
    wl["queries"].append(_query_with_params(["555123456", "topsecret", None]))
    rc, out, text = run_tool(tmp_path, wl, "--no-literals")
    assert rc == 0
    assert out is not None
    # Params retained (and identifiers in the SQL still renamed).
    assert "topsecret" in text
    assert out["queries"][-1]["params"] == ["555123456", "topsecret", None]
    assert "customers_db" not in text


def test_cluster_size_preserved(tmp_path: Any) -> None:
    # Cluster SIZE is non-sensitive config that replay must keep verbatim.
    rc, _out, text = run_tool(tmp_path, base_workload())
    assert rc == 0
    assert "100cc" in text


def test_in_place_overwrites_input(tmp_path: Any) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload(), in_place=True)
    assert rc == 0
    assert "customers_db" not in text
    assert "hunter2" not in text


def test_no_literals_keeps_literals_but_anonymizes_identifiers(tmp_path: Any) -> None:
    rc, _out, text = run_tool(tmp_path, base_workload(), "--no-literals")
    assert rc == 0
    # Literals retained...
    assert "hunter2" in text
    assert "987654321" in text
    # ...but identifiers still anonymized.
    assert "customers_db" not in text


def test_renames_object_reference_as_whole_token(tmp_path: Any) -> None:
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
    rc, out, text = run_tool(tmp_path, wl)
    assert rc == 0
    assert out is not None
    assert not _re.search(r"(?<![A-Za-z0-9_])zorp(?![A-Za-z0-9_])", text), text
    assert not _re.search(r"(?<![A-Za-z0-9_])zorpcol(?![A-Za-z0-9_])", text), text


def test_repeated_name_renamed_consistently(tmp_path: Any) -> None:
    # A column name shared across two identically structured tables (as a
    # multi-tenant source produces) must rename to the SAME anonymized name in
    # both — otherwise the structural column list and the rewritten SQL diverge
    # and a source's TEXT COLUMNS can name a column its subsource no longer has.
    wl = base_workload()
    public = wl["databases"]["customers_db"]["public"]
    public["tables"]["events_a"] = {
        "create_sql": "CREATE TABLE events_a (shared_col int)",
        "columns": [{"name": "shared_col", "type": "int4"}],
        "rows": 1,
    }
    public["tables"]["events_b"] = {
        "create_sql": "CREATE TABLE events_b (shared_col int)",
        "columns": [{"name": "shared_col", "type": "int4"}],
        "rows": 1,
    }
    rc, out, _text = run_tool(tmp_path, wl)
    assert rc == 0
    assert out is not None
    tables = out["databases"]["db_0"]["public"]["tables"]
    # The two events tables are the single-column ones (orders has two columns).
    event_cols = [
        t["columns"][0]["name"] for t in tables.values() if len(t["columns"]) == 1
    ]
    assert len(event_cols) == 2, event_cols
    assert event_cols[0] == event_cols[1], f"renamed inconsistently: {event_cols}"
    assert event_cols[0] != "shared_col", "column not anonymized"


def test_no_output_target_errors(
    tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    # Errors at argument resolution, before the helper is consulted.
    inp = tmp_path / "workload.yml"
    inp.write_text(yaml.safe_dump(base_workload()))
    with mock.patch.object(sys, "argv", ["mz-workload-anonymize", str(inp)]):
        rc = mz_workload_anonymize.main()
    assert rc == 1
    assert "in-place" in capsys.readouterr().err


def test_missing_helper_errors(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The parser is mandatory: with no helper binary the tool must refuse to run
    # rather than emit un-anonymized SQL.
    monkeypatch.setattr(mz_workload_anonymize, "_locate_redactor", lambda: None)
    rc, out, _text = run_tool(tmp_path, base_workload())
    assert rc == 1
    assert out is None
    assert "mz-sql-anonymize helper not found" in capsys.readouterr().err


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


def test_subsource_child_key_is_anonymized(tmp_path: Any) -> None:
    # Regression: the child's fully-qualified dict key must not leak the
    # original database/schema/name.
    rc, _out, text = run_tool(tmp_path, cdc_workload())
    assert rc == 0
    for original in ("upstream_db", "ingest_schema", "people"):
        assert original not in text, f"{original!r} leaked via a child key"


def cross_schema_cdc_workload() -> dict[str, Any]:
    """A source in one schema whose child belongs to another, later-sorted
    schema. The child key is built in pass 1 while processing the source's
    schema (`aaa_src`), before the child's schema (`zzz_upstream`) is mapped, so
    a stale key leaks the original schema name unless rebuilt in pass 2."""
    empty = lambda: {  # noqa: E731
        "tables": {},
        "views": {},
        "materialized_views": {},
        "indexes": {},
        "types": {},
        "connections": {},
        "sources": {},
        "sinks": {},
    }
    src_schema = empty()
    src_schema["sources"] = {
        "pg_src": {
            "create_sql": "CREATE SOURCE pg_src FROM POSTGRES CONNECTION c",
            "type": "postgres",
            "children": {
                "mydb.zzz_upstream.people": {
                    "name": "people",
                    "database": "mydb",
                    "schema": "zzz_upstream",
                    "create_sql": "CREATE SUBSOURCE people (id int)",
                    "columns": [{"name": "id", "type": "int4"}],
                },
            },
        },
    }
    return {
        "clusters": {},
        "databases": {"mydb": {"aaa_src": src_schema, "zzz_upstream": empty()}},
        "queries": [],
    }


def test_subsource_child_key_anonymized_across_schemas(tmp_path: Any) -> None:
    # Regression for the pass-1 ordering bug: a child whose schema is mapped
    # only after its source must still have its key rebuilt with the mapped name.
    rc, _out, text = run_tool(tmp_path, cross_schema_cdc_workload())
    assert rc == 0
    assert "zzz_upstream" not in text, "child's upstream schema leaked via its key"


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


def test_verify_catches_numeric_string_literal() -> None:
    # Finding 1 backstop: a bare numeric STRING (an SSN/account/card number) is
    # data and must be flagged. It must NOT be exempted as a number or interval.
    for value in ("123456789", "4111111111111111", "555 1234"):
        new = {
            "databases": {},
            "clusters": {},
            "queries": [{"sql": f"SELECT * FROM t WHERE x = '{value}'"}],
        }
        args = mock.Mock(identifiers=True, literals=True)
        problems = mz_workload_anonymize.verify_anonymized(new, {}, args)
        assert any(value in p for p in problems), f"{value!r} not flagged"


def test_verify_exempts_interval_value_part() -> None:
    # The value part of an `INTERVAL '…'` literal is a duration the helper keeps
    # verbatim; it surfaces as a bare-number literal but is config, so the verify
    # pass must accept it, in that position only.
    new = {
        "databases": {},
        "clusters": {},
        "queries": [
            {"sql": "SELECT * FROM t WHERE created > now() - INTERVAL '60' DAY"},
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_still_accepts_real_durations() -> None:
    # The tightening must not start flagging genuine number-bearing durations.
    for value in ("5 minutes", "500ms", "0.1s", "00:05:00", "7 days"):
        new = {
            "databases": {},
            "clusters": {},
            "queries": [{"sql": f"SELECT * FROM t WHERE note = '{value}'"}],
        }
        args = mock.Mock(identifiers=True, literals=True)
        problems = mz_workload_anonymize.verify_anonymized(new, {}, args)
        assert problems == [], f"{value!r} wrongly flagged: {problems}"


def test_verify_accepts_redacted_placeholder() -> None:
    new = {
        "databases": {},
        "clusters": {},
        "queries": [
            {"sql": "SELECT * FROM t WHERE a = '<REDACTED>' AND b = '<REDACTED>'"},
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_accepts_consistent_rename_token() -> None:
    # DDL string values are consistently renamed to a stable `redacted_<hash>`
    # token (kept as a valid literal so replay stays linked); verify must accept
    # it as redacted just like the inert `<REDACTED>` placeholder.
    new = {
        "databases": {},
        "clusters": {},
        "queries": [
            {"sql": "SELECT * FROM t WHERE a = 'redacted_deadbeefcafef00d'"},
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_accepts_renamed_object_name_literal() -> None:
    # A renamed object name (a non-identity mapping value) can legitimately
    # appear as a string literal — e.g. an upstream database that also shows up
    # as an external reference. Verify must accept it as anonymized.
    new = {
        "databases": {},
        "clusters": {},
        "queries": [
            {"sql": "CREATE CONNECTION c TO POSTGRES (DATABASE 'object_46')"},
        ],
    }
    mapping = {"upstream_prod_db": "object_46"}
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, mapping, args) == []


def test_verify_flags_identity_mapped_keyword_literal() -> None:
    # An identity mapping (a kept keyword/builtin name mapped to itself) is NOT a
    # rename, so a data literal that merely spells it (e.g. a JSON key 'id') must
    # still be flagged — it was not anonymized.
    new = {
        "databases": {},
        "clusters": {},
        "queries": [{"sql": "SELECT x WHERE type = 'id'"}],
    }
    mapping = {"id": "id"}
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, mapping, args)
    assert any("'id'" in p for p in problems)


def test_verify_flags_legacy_literal_placeholder() -> None:
    # The old regex `'literal_N'` style is no longer produced; verify must not
    # treat it as anonymized.
    new = {
        "databases": {},
        "clusters": {},
        "queries": [{"sql": "SELECT * FROM t WHERE a = 'literal_1'"}],
    }
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, {}, args)
    assert any("literal_1" in p for p in problems)


def test_verify_catches_unanonymized_bound_param() -> None:
    new = {
        "clusters": {},
        "databases": {},
        "queries": [{"sql": "SELECT * FROM t WHERE id = $1", "params": ["42"]}],
    }
    args = mock.Mock(identifiers=True, literals=True)
    problems = mz_workload_anonymize.verify_anonymized(new, {}, args)
    assert any("42" in p for p in problems)


def test_verify_accepts_redacted_bound_params() -> None:
    new = {
        "clusters": {},
        "databases": {},
        "queries": [
            {
                "sql": "SELECT * FROM t WHERE id = $1 AND y = $2",
                "params": ["<REDACTED>", None],
            }
        ],
    }
    args = mock.Mock(identifiers=True, literals=True)
    assert mz_workload_anonymize.verify_anonymized(new, {}, args) == []


def test_verify_ignores_bound_params_when_literals_off() -> None:
    # With --no-literals, params are kept and must not be flagged.
    new = {
        "clusters": {},
        "databases": {},
        "queries": [{"sql": "SELECT * FROM t WHERE id = $1", "params": ["42"]}],
    }
    args = mock.Mock(identifiers=True, literals=False)
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
