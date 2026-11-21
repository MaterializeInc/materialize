# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis_driver import read_only
from materialize.antithesis_driver.client import Status
from materialize.antithesis_driver.draw import SeededDraw
from materialize.orchestratord import EnvironmentdPod

ACKNOWLEDGES = "read-only isolation: a never-promoted environmentd generation acknowledges no write sent directly to it"


def cr(active: int, reason: str) -> dict[str, Any]:
    return {
        "status": {
            "activeGeneration": active,
            "conditions": [{"type": "UpToDate", "reason": reason}],
        }
    }


class Connection:
    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, statement: bytes) -> None:
        pass


def component(reads: list[dict[str, Any]]) -> read_only.ReadOnly:
    """A component whose CR reads return `reads` in turn, next to a leader of
    generation 3 and a candidate of generation 4."""
    pods = [
        EnvironmentdPod(3, "mzx-environmentd-3-0", "u3", "10.0.0.3", 0, True),
        EnvironmentdPod(4, "mzx-environmentd-4-0", "u4", "10.0.0.4", 0, True),
    ]
    materialize = SimpleNamespace(
        get=lambda: reads.pop(0), environmentd_pods=lambda: pods
    )
    pool = SimpleNamespace(sessions=[None], call=lambda i, op, wait: Status.DONE)
    ctx = SimpleNamespace(quiesced=threading.Event(), sessions=pool)
    kube = SimpleNamespace(env=SimpleNamespace(materialize=materialize))
    return read_only.ReadOnly(ctx, kube)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    "after,holds",
    [
        # The candidate was never told to promote, so its acknowledgement is
        # a write by a read-only process.
        ("ReadyToPromote", False),
        # During Promoting the candidate may already be the leader.
        ("Promoting", True),
    ],
)
def test_an_acknowledged_direct_write_is_blamed_only_on_a_never_promoted_generation(
    monkeypatch: pytest.MonkeyPatch, after: str, holds: bool
) -> None:
    calls: list[tuple[bool, str]] = []
    monkeypatch.setattr(
        read_only,
        "always_or_unreachable",
        lambda condition, message, details=None: calls.append((condition, message)),
    )
    monkeypatch.setattr(read_only, "sometimes", lambda *args: None)
    monkeypatch.setattr(read_only.psycopg, "connect", lambda **kwargs: Connection())
    c = component([cr(3, "Applying"), cr(3, after)])
    assert c.probe_write(SeededDraw(1)) == Status.DONE
    assert calls == [(holds, ACKNOWLEDGES)]


def test_a_probe_across_a_promotion_judges_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        read_only, "always_or_unreachable", lambda *args: calls.append(args)
    )
    monkeypatch.setattr(read_only, "sometimes", lambda *args: None)
    monkeypatch.setattr(read_only.psycopg, "connect", lambda **kwargs: Connection())
    c = component([cr(3, "Applying"), cr(4, "Applied")])
    assert c.probe_write(SeededDraw(1)) == Status.DONE
    assert not calls
