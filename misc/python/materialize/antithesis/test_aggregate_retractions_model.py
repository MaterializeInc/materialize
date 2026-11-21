# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import random

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import aggregate_retractions_model as model


def assert_product(condition, message, details):
    assert condition, (message, details)


def test_exact_components_do_not_carry_days_or_months_and_preserve_null_counts():
    expected = model.evaluate(
        [(0, (1, 40, 1)), (0, (1, 40, 1)), (0, (-1, -20, -1)), (0, None), (1, None)]
    )
    assert expected.rows == ((0, 1, 60, 1, 3, 4), (1, None, None, None, 0, 1))
    assert not expected.overflows


@pytest.mark.parametrize("axis,bits", list(enumerate(model.BITS)))
@pytest.mark.parametrize("sign", [-1, 1])
def test_overflow_can_be_repaired_by_exact_retraction(axis, bits, sign):
    components = [0, 0, 0]
    components[axis] = sign * ((1 << (bits - 1)) - 1)
    value = (components[0], components[1], components[2])
    assert model.evaluate([(0, value), (0, value)]).overflows == (
        (0, model.AXES[axis]),
    )
    assert model.evaluate([(0, value)]).rows == ((0, *components, 1, 1),)
    assert not model.evaluate(
        [(0, value), (0, (-value[0], -value[1], -value[2]))]
    ).overflows


def test_generated_plans_cover_all_components_with_bounded_representable_inputs():
    axes = set()
    for seed in range(100):
        plan = model.generate(random.Random(seed))
        axes.add(plan.axis)
        assert len(plan.baseline) <= 33
        assert not model.evaluate(plan.baseline).overflows
        assert model.evaluate((*plan.baseline, *plan.poison)).overflows == (
            (0, plan.axis),
        )
        assert not model.evaluate((*plan.baseline, plan.extra)).overflows
        for _, value in (*plan.baseline, *plan.poison, plan.extra):
            if value is not None:
                assert all(
                    -(1 << (bits - 1)) <= v < (1 << (bits - 1))
                    for bits, v in zip(model.BITS, value)
                )
    assert axes == set(model.AXES)
