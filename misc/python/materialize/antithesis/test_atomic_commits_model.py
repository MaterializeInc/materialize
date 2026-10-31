# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.antithesis.atomic_commits_model import Plan, violations


@pytest.mark.parametrize(
    "rows,outcome,uncommitted,error",
    [
        ([(0, 1, 7)], "ok", None, "torn"),
        ([], "ok", None, "lost"),
        ([], "observed", None, "lost"),
        ([(0, 1, 7), (1, 1, 7)], "rejected", None, "rolled-back"),
        ([(0, 1, 7), (1, 1, 7)], "indeterminate", 1, "uncommitted"),
        ([(0, 1, 7), (1, 1, 8)], "ok", None, "payload"),
        ([(0, 1, 7), (1, 1, 7), (1, 1, 7)], "ok", None, "duplicate"),
        ([(0, 1, 7), (1, 1, 7), (2, 2, 7)], "ok", None, "unattempted"),
        ([(0, 1, 7), (1, 1, 7), (2, 1, 7)], "ok", None, "payload"),
    ],
)
def test_rejects_transaction_corruption(rows, outcome, uncommitted, error):
    errors = violations({1: Plan((0, 1), 7, outcome)}, rows, uncommitted=uncommitted)
    assert any(error in item for item in errors)


@pytest.mark.parametrize("visible", [False, True])
def test_indeterminate_transaction_is_all_or_none(visible):
    rows = [(0, 1, 7), (2, 1, 7)] if visible else []
    assert not violations({1: Plan((0, 2), 7, "indeterminate")}, rows)


def test_retired_fixture_cleanup_may_delete_tables_separately():
    plan = Plan((0, 1), 7, "ok", retired=True)
    assert not violations({1: plan}, [(0, 1, 7)])
    assert violations({1: plan}, [(0, 1, 8)])
    assert violations({1: plan}, [(0, 1, 7), (0, 1, 7)])


def test_overlapping_transaction_markers_are_checked_independently():
    plans = {
        1: Plan((0, 1), 4, "ok"),
        2: Plan((1, 2), 4, "indeterminate"),
        3: Plan((0, 2), -8, "rejected"),
    }
    rows = [(0, 1, 4), (1, 1, 4), (1, 2, 4), (2, 2, 4)]
    assert not violations(plans, rows)
    assert violations(plans, rows[:-1])
