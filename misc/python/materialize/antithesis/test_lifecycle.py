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
from contextlib import contextmanager
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import sql
from materialize.antithesis.drivers import lifecycle

NO_REPLICAS = 'CLUSTER "lifecycle_c1" has no replicas available to service request'


def test_lifecycle_index_on_zero_replica_cluster_is_an_expected_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE rows (tbl TEXT, attempted INT, acked INT)")
    session = Mock(host="host", db=db)
    objects = {
        "t1": {
            "id": "u1",
            "type": "table",
            "replacement": False,
            "cluster_id": None,
        },
        "i_t1": {
            "id": "u3",
            "type": "index",
            "replacement": False,
            "on_id": "u1",
            "cluster_id": "u2",
        },
    }
    monkeypatch.setattr(lifecycle, "live_objects", lambda _: objects)
    monkeypatch.setattr(
        lifecycle,
        "live_clusters",
        lambda _: {"lifecycle_c1": {"id": "u2", "replicas": 0}},
    )
    error = psycopg.errors.FeatureNotSupported(NO_REPLICAS)
    assert sql.classify(error).outcome is sql.Outcome.VIOLATION

    @contextmanager
    def connection(host: str, **kwargs: Any):
        if kwargs["options"]["cluster"] == "lifecycle_c1":
            raise error
        conn = Mock()
        conn.execute.return_value.fetchone.return_value = (0,)
        yield conn

    def check(condition: bool, message: str, details: Any) -> None:
        assert condition, (message, details)

    monkeypatch.setattr(sql, "connection", connection)
    monkeypatch.setattr(lifecycle, "always_or_unreachable", check)
    assert lifecycle.check_readable(session, "create_index", require_success=True) == {
        "ok": 1,
        "transient": 1,
        "origin": "create_index",
    }
    db.close()


@pytest.mark.parametrize(
    "error",
    [
        psycopg.errors.FeatureNotSupported("unexpected unsupported read"),
        psycopg.errors.InternalError_(NO_REPLICAS),
        psycopg.errors.InternalError_('relation "t1" is not readable at any timestamp'),
    ],
)
def test_lifecycle_reads_preserve_unexpected_errors(
    monkeypatch: pytest.MonkeyPatch, error: psycopg.Error
) -> None:
    monkeypatch.setattr(sql, "connection", Mock(side_effect=error))
    count, classified = lifecycle.read_count(Mock(host="host"), "t1", "lifecycle_c1")
    assert count == 0
    assert classified is not None
    assert classified.outcome is sql.Outcome.VIOLATION
