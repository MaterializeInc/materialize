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
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest
import urllib3

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


def test_restart_signature_is_bounded_and_tolerates_an_unreachable_api_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = Mock()
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    monkeypatch.setattr(lifecycle.client, "CoreV1Api", lambda: core)
    env = SimpleNamespace(endpoints=SimpleNamespace(namespace="mz"))
    assert lifecycle.safe_signature(env) == "[]"  # type: ignore[arg-type]
    timeout = core.list_namespaced_pod.call_args.kwargs["_request_timeout"]
    assert type(timeout) is int and timeout > 0
    # The client reports a refused or reset connection as a urllib3 error.
    core.list_namespaced_pod.side_effect = urllib3.exceptions.ProtocolError("reset")
    assert lifecycle.safe_signature(env) is None  # type: ignore[arg-type]


def snapshot(replicas: list[list[str]], rf: str = "2") -> dict[str, Any]:
    return {
        "items": [],
        "global_ids": [],
        "shards": [],
        "retention": [],
        "replacements": [],
        "indexes": [],
        "clusters": [
            ["u1", "lifecycle_c1", "True", "antithesis-1", rf],
            ["u2", "lifecycle_u2", "False", "None", "None"],
        ],
        "replicas": replicas,
        "reconfigurations": [],
        "dump": None,
    }


def failed_families(
    monkeypatch: pytest.MonkeyPatch, before: dict[str, Any], after: dict[str, Any]
) -> list[str]:
    failed = []

    def check(condition: bool, message: str, details: Any) -> None:
        if not condition:
            failed.append(details["family"])

    monkeypatch.setattr(lifecycle, "always_or_unreachable", check)
    lifecycle.compare_projections(before, after, "test")
    return failed


def test_replicas_the_cluster_controller_still_owes_are_not_compared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r1 = ["u10", "r1", "u1", "antithesis-1"]
    r2 = ["u11", "r2", "u1", "antithesis-1"]
    unmanaged = ["u12", "r1", "u2", "antithesis-1"]
    # The snapshot follows an acknowledged `SET (REPLICATION FACTOR 2)`, which
    # the controller reconciles in a later catalog transaction.
    assert not failed_families(
        monkeypatch, snapshot([r1, unmanaged]), snapshot([r1, r2, unmanaged])
    )
    # A cut-over whose old replica is not dropped yet.
    old = ["u9", "r0", "u1", "antithesis-2"]
    assert not failed_families(
        monkeypatch, snapshot([old, r1, r2, unmanaged]), snapshot([r1, r2, unmanaged])
    )
    # Reconciled clusters and unmanaged clusters are still compared.
    replaced = ["u13", "r2", "u1", "antithesis-1"]
    assert failed_families(
        monkeypatch, snapshot([r1, r2, unmanaged]), snapshot([r1, replaced, unmanaged])
    ) == ["replicas"]
    assert failed_families(
        monkeypatch, snapshot([r1, r2, unmanaged]), snapshot([r1, r2])
    ) == ["replicas"]


@pytest.mark.parametrize(
    "outcome,columns",
    [(None, "k, v, c7"), (sql.Outcome.INDETERMINATE, "k, v")],
)
def test_add_column_chain_selects_the_column_only_after_an_acknowledged_alter(
    monkeypatch: pytest.MonkeyPatch, outcome: sql.Outcome | None, columns: str
) -> None:
    statements: list[str] = []

    def ddl(family: str, text: str, timeout_ms: int | None = None):
        statements.append(text)
        return outcome if family == "ddl_alter_table" else None

    objects = {"t1": {"id": "u1", "type": "table", "replacement": None}}
    driver = object.__new__(lifecycle.Driver)
    driver.s = Mock(ddl=ddl)
    monkeypatch.setattr(lifecycle, "next_id", lambda db, name: 7)
    monkeypatch.setattr(lifecycle, "live_objects", lambda s: objects)
    monkeypatch.setattr(lifecycle, "compute_clusters", lambda s: ["quickstart"])
    monkeypatch.setattr(lifecycle.rng, "random", lambda: 1.0)
    driver.add_column(objects, "t1", remaining=1)
    assert statements[0].endswith("ADD COLUMN c7 text")
    assert f"AS SELECT {columns} FROM" in statements[1]
