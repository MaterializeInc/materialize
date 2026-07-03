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
