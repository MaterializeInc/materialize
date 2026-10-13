# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from typing import Any

from materialize.antithesis.remap_frontiers import Frontiers, Verdict, classify


def report(
    since: list[int],
    upper: list[int],
    controller: list[int] | None = None,
    tombstone: bool = False,
) -> dict[str, Any]:
    return {
        "shard_id": "s",
        "initialized": True,
        "tombstone": tombstone,
        "since": since,
        "upper": upper,
        "controller_critical_since": controller,
        "critical_sinces": {},
        "leased_sinces": {},
    }


def f(*args: Any, **kwargs: Any) -> Frontiers:
    parsed = Frontiers.parse(report(*args, **kwargs))
    assert parsed is not None
    return parsed


def test_parse() -> None:
    assert Frontiers.parse({"shard_id": "s", "initialized": False}) == Frontiers(
        False, False, (), (), None
    )
    assert Frontiers.parse({"shard_id": "s", "initialized": True}) is None
    assert f([3], [5], [4]).effective_since == (4,)
    assert f([3], [5]).effective_since == (3,)


def test_remap_since_below_export_upper_is_ok() -> None:
    check = classify(f([9], [20]), f([9], [10]))
    assert check.verdict is Verdict.OK
    assert check.headroom_ms == 1
    assert not check.violated


def test_remap_since_at_export_upper_is_a_violation() -> None:
    check = classify(f([10], [20]), f([10], [10]))
    assert check.verdict is Verdict.PAST_UPPER_AHEAD
    assert check.violated
    assert check.headroom_ms == 0


def test_ss346_shape_has_export_since_behind() -> None:
    # The SS-346 report: dependent since [0], upper below the dependency since.
    check = classify(f([1783940527000], [1783940600000]), f([0], [1783940337252]))
    assert check.verdict is Verdict.PAST_UPPER_BEHIND
    assert check.export_since_behind


def test_controller_since_is_the_stricter_read() -> None:
    # A leased reader holds the shard since back, the controller's critical
    # since has passed the export upper.
    check = classify(f([5], [20], controller=[12]), f([8], [10]))
    assert check.verdict is Verdict.PAST_UPPER_BEHIND
    assert check.remap_since == (12,)


def test_empty_remap_since_is_sql603() -> None:
    check = classify(f([], [20]), f([3], [10]))
    assert check.verdict is Verdict.EMPTY_SINCE
    assert check.violated
    assert check.headroom_ms is None


def test_unwritten_or_sealed_exports_are_skipped() -> None:
    assert classify(f([50], [60]), f([0], [0])).verdict is Verdict.SKIP
    assert classify(f([50], [60]), f([3], [])).verdict is Verdict.SKIP
    assert classify(f([], []), f([3], [])).verdict is Verdict.SKIP


def test_tombstoned_or_uninitialized_shards_are_skipped() -> None:
    uninit = Frontiers(False, False, (), (), None)
    assert classify(uninit, f([0], [10])).verdict is Verdict.SKIP
    assert classify(f([5], [10]), uninit).verdict is Verdict.SKIP
    assert classify(f([], [], tombstone=True), f([3], [10])).verdict is Verdict.SKIP
