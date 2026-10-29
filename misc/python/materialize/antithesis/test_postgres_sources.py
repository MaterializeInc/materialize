# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import postgres_sources as p


@pytest.mark.parametrize(
    "corruption", [None, "aborted", "swapped_sequence", "wrong_kind"]
)
def test_ledger_rejects_corrupted_transaction_metadata(
    monkeypatch: pytest.MonkeyPatch, corruption: str | None
) -> None:
    with sqlite3.connect(":memory:") as db:
        db.execute(
            "CREATE TABLE txns (txn_id TEXT, slot INTEGER, lsn_before INTEGER, lsn_after INTEGER, status TEXT)"
        )
        db.execute("CREATE TABLE ops (txn_id TEXT, tbl TEXT, seq INTEGER, kind TEXT)")
        db.executemany(
            "INSERT INTO txns VALUES (?, 0, 0, NULL, ?)",
            [("a", "aborted" if corruption == "aborted" else "acked"), ("b", "acked")],
        )
        db.executemany(
            "INSERT INTO ops VALUES (?, 's0.t0', ?, 'dml')", [("a", 1), ("b", 2)]
        )
        rows = [("a", "s0.t0", 1, "dml"), ("b", "s0.t0", 2, "dml")]
        if corruption == "swapped_sequence":
            rows = [("a", "s0.t0", 2, "dml"), ("b", "s0.t0", 1, "dml")]
        elif corruption == "wrong_kind":
            rows[0] = ("a", "s0.t0", 1, "truncate")
        assertions: list[bool] = []
        monkeypatch.setattr(p, "always", lambda ok, *args: assertions.append(ok))
        monkeypatch.setattr(p, "sometimes", lambda *args: None)

        ledger = p._check_ledger(db, 0, 10, rows, {})
        assert (ledger is not None) == (corruption is None)
        assert all(assertions) == (corruption is None)
