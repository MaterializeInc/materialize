# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for workload replay helpers."""

from __future__ import annotations

import random
import threading
from unittest.mock import MagicMock, Mock

import psycopg
import pymysql
import pytest

from materialize.cli.mz_workload_capture import parse_mz_list
from materialize.workload_replay import data
from materialize.workload_replay import ingest as ingest_module
from materialize.workload_replay.column import Column
from materialize.workload_replay.replay import pg_params_to_psycopg


def test_pg_params_to_psycopg_rewrites_placeholders() -> None:
    sql, params = pg_params_to_psycopg(
        "SELECT * FROM t WHERE id = $1 AND note = $2", ["7", "hi"]
    )
    assert sql == "SELECT * FROM t WHERE id = %s AND note = %s"
    assert params == ["7", "hi"]


def test_pg_params_to_psycopg_binds_redacted_params_as_null() -> None:
    # Anonymized parameters carry the '<REDACTED>' placeholder; replay no longer
    # knows the value or type, so it binds NULL (as it does for redacted SQL
    # literals).
    sql, params = pg_params_to_psycopg(
        "SELECT * FROM t WHERE id = $1 AND note = $2", ["<REDACTED>", "kept"]
    )
    assert params == [None, "kept"]


def test_parse_mz_list_handles_empty_and_quoting() -> None:
    # An empty list must be [], not [""] (which renders as an invalid
    # "SET search_path =" during replay).
    assert parse_mz_list("{}") == []
    assert parse_mz_list("{public}") == ["public"]
    assert parse_mz_list("{public,other}") == ["public", "other"]
    # Quoted elements may contain commas and escaped quotes.
    assert parse_mz_list('{public,"weird,name"}') == ["public", "weird,name"]
    assert parse_mz_list(r'{"a\"b"}') == ['a"b']


def test_column_value_does_not_emit_default_expr_into_copy() -> None:
    # The captured default is a SQL expression. It is fine spliced into SQL, but
    # as a COPY text datum it would be inserted literally (e.g. the text
    # "now()") and fail to parse, so value(in_query=False) must never return it.
    col = Column("c", "timestamp with time zone", False, "now()", None)
    copy_values = {
        col.value(random.Random(seed), in_query=False) for seed in range(200)
    }
    assert "now()" not in copy_values
    sql_values = {col.value(random.Random(seed), in_query=True) for seed in range(200)}
    assert "now()" in sql_values


def test_kafka_value_is_raw_and_never_a_default_expr() -> None:
    # Avro-serialized fields and webhook bodies need raw values, not the SQL
    # default expression and not SQL-quoted strings.
    col = Column("c", "text", False, "'x'::text", None)
    values = {col.kafka_value(random.Random(seed)) for seed in range(200)}
    assert "'x'::text" not in values
    assert all(not v.startswith("'") for v in values if isinstance(v, str))


@pytest.mark.parametrize("source_type", ["postgres", "mysql"])
@pytest.mark.parametrize("fail_write", [False, True])
def test_continuous_ingestion_reuses_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
    source_type: str,
    fail_write: bool,
) -> None:
    stop = threading.Event()
    c = Mock()
    c.default_port.return_value = 5432
    conn = MagicMock(closed=False, open=True)
    connect = Mock(return_value=conn)
    monkeypatch.setattr(ingest_module.psycopg, "connect", connect)
    monkeypatch.setattr(ingest_module.pymysql, "connect", connect)
    batches = 0
    write_error = (
        psycopg.OperationalError
        if source_type == "postgres"
        else pymysql.err.OperationalError
    )

    def cursor():
        nonlocal batches
        batches += 1
        if batches == 3:
            stop.set()
            if fail_write:
                if source_type == "mysql":
                    conn.open = False
                    conn.close.side_effect = pymysql.err.Error("Already closed")
                raise write_error("injected write failure")
        return MagicMock()

    conn.cursor.side_effect = cursor
    reference = "db.s.t" if source_type == "postgres" else "db.t"
    child = {
        "type": "table",
        "create_sql": f"CREATE TABLE t FROM SOURCE src (REFERENCE = {reference})",
        "messages_second": 10000,
        "columns": [
            {"name": "c", "type": "integer", "nullable": False, "default": None}
        ],
    }
    source = {"type": source_type, "children": {"t": child}}
    workload = {"databases": {"db": {"s": {"sources": {"source": source}}}}}
    threads = data.create_ingestions(
        c, workload, stop, 1, False, {"total": 0, "slow": 0, "failed": 0}
    )
    thread = threads[0]
    thread.start()
    try:
        if fail_write:
            with pytest.raises(write_error, match="injected write failure"):
                thread.join(timeout=5)
        else:
            thread.join(timeout=5)
        assert not thread.is_alive()
    finally:
        stop.set()
        if thread.is_alive():
            thread.join(timeout=5)
    assert batches == 3
    assert connect.call_count == 1
    if source_type == "mysql" and fail_write:
        conn.close.assert_not_called()
    else:
        conn.close.assert_called_once_with()


@pytest.mark.parametrize("source_type", ["postgres", "mysql"])
def test_reference_connection_replaces_closed_connection(source_type: str) -> None:
    old = MagicMock(closed=False, open=True)
    new = MagicMock(closed=False, open=True)
    connect = Mock(side_effect=[old, new])
    key = (source_type, 5432, "db")
    with ingest_module.reuse_reference_connections():
        with ingest_module._reference_connection(key, connect) as conn:
            assert conn is old
        if source_type == "postgres":
            old.closed = True
        else:
            old.open = False
        with ingest_module._reference_connection(key, connect) as conn:
            assert conn is new
    assert connect.call_count == 2
    old.close.assert_not_called()
    new.close.assert_called_once_with()
