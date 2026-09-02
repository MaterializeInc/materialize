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

from materialize.cli.mz_workload_capture import parse_mz_list
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
