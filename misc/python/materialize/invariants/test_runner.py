# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from collections import Counter
from unittest.mock import Mock

import pytest

from materialize.invariants.executor import Runner
from materialize.invariants.framework import InvariantViolation
from materialize.invariants.toxiproxy import _STOPPED, Disruptor


def test_worker_progress_counts_only_acknowledged_writes():
    runner = Runner(Mock(), 600, Mock(), [])
    runner.worker_stats = [
        Counter({("write", "unconfirmed"): 40, ("write", "unknown"): 3})
    ]
    action = Mock(async_commits=0)
    runner.worker_actions = [action]
    with pytest.raises(InvariantViolation, match="no worker op ever committed"):
        runner._check_vacuity(*runner._progress())
    action.async_commits = 2
    runner.worker_stats[0][("insert", "committed")] = 1
    assert runner._progress() == ([], 3)


def test_convergence_replay_does_not_count_as_chaos_progress(monkeypatch):
    scenario = Mock(max_workers=0)
    scenario.ctx.complexity.workers = 0
    scenario.ctx.rng = random.Random(1)
    checker = Mock(validations=0)
    scenario.checkers.return_value = [checker]
    scenario.converge.side_effect = lambda: setattr(checker, "validations", 1)
    runner = Runner(scenario, 1, Mock(), [])
    runner.worker_stats = [Counter({("insert", "committed"): 1})]
    for method in ("_monitor", "_checker_loop", "_dump_diagnostics"):
        monkeypatch.setattr(runner, method, Mock())
    with pytest.raises(InvariantViolation, match="never verified"):
        runner.run()
    assert checker.validations == 1


def test_latency_filter_skips_overlapping_and_post_heal_kills(monkeypatch):
    rng = Mock()
    on_error = Mock()
    disruptor = Disruptor(
        Mock(),
        [],
        rng,
        Mock(),
        (0, 0),
        (0, 0),
        1,
        on_error,
        [Mock()],
        kind_filter={"latency"},
    )
    leg_cycle = Mock()
    process_cycle = Mock()
    monkeypatch.setattr(disruptor, "_leg_cycle", leg_cycle)
    monkeypatch.setattr(disruptor, "_process_cycle", process_cycle)
    monkeypatch.setattr(disruptor, "_heal_all_with_retries", Mock())
    monkeypatch.setattr(
        disruptor, "_wait_for_next_cycle", Mock(side_effect=[None, _STOPPED])
    )
    # Select the non-storm path, then the overlapping cycle and the post-heal kill.
    rng.random.side_effect = [0.25, 0.1, 0.25]
    rng.uniform.return_value = 0
    disruptor.run()
    leg_cycle.assert_called_once_with(1.0, overlap_kill=False)
    process_cycle.assert_not_called()
    on_error.assert_not_called()
