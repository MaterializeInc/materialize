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

from materialize.antithesis import spec_model
from materialize.antithesis_driver import claims, disrupt
from materialize.orchestratord import EnvironmentdGeneration

APPLIED = "Applied status implies the active StatefulSet runs the spec at the observed generation, and the completed request and image are the spec's"
PROMOTED_ACROSS = "Applied status for a generation promoted across a spec change during Promoting implies the active StatefulSet runs the spec (CLO-339)"
PROMOTING_SAME_REQUEST = "Applied status after a spec change during Promoting without a new requestRollout implies the active StatefulSet runs the spec (CLO-339)"


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message))

    def reached(self, message: str, details: Any = None) -> None:
        self.calls.append((True, message))


def cr(
    reason: str,
    active: int,
    *,
    image: str = "mz:1",
    request: str = "r1",
    completed: str = "r1",
    generation: int = 5,
    version: str = "100",
    status: str = "True",
) -> dict[str, Any]:
    return {
        "metadata": {"generation": generation, "resourceVersion": version},
        "spec": {"environmentdImageRef": image, "requestRollout": request},
        "status": {
            "activeGeneration": active,
            "lastCompletedRolloutRequest": completed,
            "lastCompletedRolloutEnvironmentdImageRef": image,
            "resourcesHash": "h",
            "conditions": [
                {
                    "type": "UpToDate",
                    "status": status,
                    "reason": reason,
                    "observedGeneration": generation,
                }
            ],
        },
    }


class Materialize:
    """Returns `current` from `get`, and one StatefulSet per generation."""

    def __init__(self, images: dict[int, str]) -> None:
        self.images = images
        self.current: dict[str, Any] = {}
        self.patches: list[dict[str, Any]] = []

    def get(self) -> dict[str, Any]:
        return self.current

    def environmentd_generations(self) -> list[EnvironmentdGeneration]:
        nil = spec_model.NIL_UUID
        return [
            EnvironmentdGeneration(g, f"mzx-environmentd-{g}", f"uid{g}", image, nil)
            for g, image in self.images.items()
        ]

    def patch_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        self.patches.append(spec)
        return {}


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    r = Recorder()
    monkeypatch.setattr(claims, "always", r)
    monkeypatch.setattr(claims, "always_or_unreachable", r)
    monkeypatch.setattr(claims, "reachable", r.reached)
    return r


def test_a_violated_claim_is_asserted_only_once_it_has_settled(
    recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr(claims.time, "monotonic", lambda: now[0])
    mz = Materialize({3: "mz:1"})
    c = claims.Claims()
    mz.current = cr("Applied", 3)
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    assert recorder.calls == [(True, APPLIED)]

    # The status says Applied while the active StatefulSet runs another image.
    mz.current = cr("Applied", 3, image="mz:2", version="101")
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    assert c.pending and len(recorder.calls) == 1
    now[0] += claims.STATUS_SETTLE_SECONDS
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    assert recorder.calls[-1] == (False, APPLIED)


def test_a_claim_read_across_a_cr_change_is_not_judged(recorder: Recorder) -> None:
    mz = Materialize({3: "mz:1"})
    c = claims.Claims()
    read = cr("Applied", 3, image="mz:2")
    mz.current = cr("Applied", 3, image="mz:2", version="101")
    c.observe(mz, read)  # pyright: ignore[reportArgumentType]
    assert not recorder.calls and not c.pending


def test_a_promotion_across_a_spec_change_gets_the_known_cause(
    recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr(claims.time, "monotonic", lambda: now[0])
    mz = Materialize({3: "mz:1", 4: "mz:1"})
    c = claims.Claims()
    mz.current = cr("Promoting", 3, status="False", request="r2")
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    # Generation 4 was applied with mz:1 and is promoted at a spec of mz:2.
    mz.current = cr(
        "Applied", 4, image="mz:2", request="r2", completed="r2", generation=6
    )
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    now[0] += claims.STATUS_SETTLE_SECONDS
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    assert recorder.calls[-1] == (False, PROMOTED_ACROSS)
    assert (False, APPLIED) not in recorder.calls


def test_the_drivers_own_patch_during_promoting_names_its_request_mode(
    recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr(claims.time, "monotonic", lambda: now[0])
    mz = Materialize({4: "mz:1"})
    c = claims.Claims()
    c.patched_while_promoting(4, "keep")
    mz.current = cr("Applied", 4, image="mz:2")
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    now[0] += claims.STATUS_SETTLE_SECONDS
    c.observe(mz, mz.current)  # pyright: ignore[reportArgumentType]
    assert recorder.calls[-1] == (False, PROMOTING_SAME_REQUEST)


def disruptions(mz: Materialize) -> disrupt.Disruptions:
    d = disrupt.Disruptions.__new__(disrupt.Disruptions)
    d._lock = threading.Lock()
    d.claims = claims.Claims()
    d.kube = SimpleNamespace(  # pyright: ignore[reportAttributeAccessIssue]
        env=SimpleNamespace(
            materialize=mz,
            endpoints=SimpleNamespace(
                initial_environmentd_image="mz:1", upgrade_pending_image="mz:2"
            ),
        )
    )
    return d


def rollout(cell: spec_model.Cell) -> disrupt.Rollout:
    return disrupt.Rollout(
        request="r2",
        strategy="WaitUntilReady",
        started=0.0,
        kill_phase=None,
        kill_target="active",
        grace=1,
        image=None,
        patch=disrupt.SpecPatch(cell, seed=7, serial=9),
        prior={"requestRollout": "r1", "forceRollout": "r1"},
    )


def test_a_fresh_request_patch_is_followed_and_repeats_identically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(disrupt, "reachable", lambda message, details=None: None)
    mz = Materialize({3: "mz:1"})
    d = disruptions(mz)
    r = rollout(spec_model.Cell("force_rollout", "Applying", "fresh", None))
    assert r.patch is not None and d.kube is not None
    current = cr("Applying", 3, status="False", request="r2")
    d._patch_spec(d.kube, r, r.patch, current)
    d._patch_spec(d.kube, r, r.patch, current)
    first, second = mz.patches
    # A retry after a lost response must not start yet another rollout.
    assert first == second
    assert set(first) == {"forceRollout", "requestRollout"}
    assert r.request == first["requestRollout"] != "r2"


def test_a_rendered_change_during_promoting_is_attributed_before_it_is_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(disrupt, "reachable", lambda message, details=None: None)
    mz = Materialize({3: "mz:1"})

    def lost(spec: dict[str, Any]) -> dict[str, Any]:
        raise OSError("response lost")

    mz.patch_spec = lost  # pyright: ignore[reportAttributeAccessIssue]
    d = disruptions(mz)
    r = rollout(spec_model.Cell("force_rollout", "Promoting", "keep", None))
    assert r.patch is not None and d.kube is not None
    with pytest.raises(OSError):
        d._patch_spec(
            d.kube, r, r.patch, cr("Promoting", 3, status="False", request="r2")
        )
    assert d.claims._promoting_patch == {4: "keep"}
    assert r.request == "r2"


def test_a_revert_restores_the_spec_from_before_the_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(disrupt, "reachable", lambda message, details=None: None)
    mz = Materialize({3: "mz:1"})
    d = disruptions(mz)
    r = rollout(spec_model.Cell("revert", "Applying", "restore", None))
    assert r.patch is not None and d.kube is not None
    d._patch_spec(d.kube, r, r.patch, cr("Applying", 3, status="False", request="r2"))
    (changes,) = mz.patches
    assert changes["requestRollout"] == "r1" and changes["forceRollout"] == "r1"
    assert r.request == "r1"
