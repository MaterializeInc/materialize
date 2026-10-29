# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import spec_phase


def test_manual_promotion_retries_an_unacknowledged_patch() -> None:
    snap = SimpleNamespace(
        reason="ReadyToPromote",
        request="r",
        spec={"rolloutStrategy": "ManuallyPromote"},
    )
    ctx = MagicMock()
    ctx.poll.side_effect = [snap, snap]
    with (
        patch.object(spec_phase, "settled", side_effect=[False, True]),
        patch.object(
            spec_phase.rollouts, "submit_patch", side_effect=[("r", None), ("r", snap)]
        ) as submit,
        patch.object(spec_phase.time, "sleep"),
    ):
        final, _ = spec_phase.follow_until_settled(ctx, None, 10)
    assert final is snap
    assert submit.call_count == 2


def test_eventual_settle_retries_unacknowledged_normalization() -> None:
    ctx = MagicMock()
    snap = SimpleNamespace(
        active=1, reason="Applied", summary=lambda: {}, tracked_spec=lambda: {}
    )
    ctx.kube.snapshot.return_value = snap
    now = [0.0]

    def watch(*args: object) -> None:
        now[0] += 5

    with (
        patch.object(spec_phase, "Environment"),
        patch.object(spec_phase.rollouts, "operator_ctx", return_value=ctx),
        patch.object(spec_phase.rollouts, "observer_db"),
        patch.object(spec_phase.rollouts, "CrWatcher"),
        patch.object(spec_phase.rollouts, "StatefulSetWatcher"),
        patch.object(spec_phase.rollouts, "run_watches", side_effect=watch),
        patch.object(
            spec_phase.rollouts, "promoted_across_spec_change", return_value=set()
        ),
        patch.object(
            spec_phase.rollouts, "normalize", side_effect=[False, True]
        ) as normalize,
        patch.object(spec_phase, "settled", side_effect=[False, True]),
        patch.object(spec_phase.time, "monotonic", side_effect=lambda: now[0]),
        patch.object(spec_phase, "judge_claim"),
    ):
        spec_phase.eventually_main()
    assert normalize.call_count == 2
