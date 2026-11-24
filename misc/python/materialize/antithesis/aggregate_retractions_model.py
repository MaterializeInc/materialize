# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Exact interval component sums, including grouped overflow and NULL semantics."""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

Interval = tuple[int, int, int]
Input = tuple[int, Interval | None]
Result = tuple[int, int | None, int | None, int | None, int, int]
AXES = ("months", "days", "micros")
BITS = (32, 32, 64)


@dataclass(frozen=True)
class Expected:
    rows: tuple[Result, ...]
    overflows: tuple[tuple[int, str], ...]


def evaluate(rows: Iterable[Input]) -> Expected:
    groups: dict[int, list[Interval | None]] = defaultdict(list)
    for group, value in rows:
        groups[group].append(value)
    output: list[Result] = []
    overflows = []
    for group, values in sorted(groups.items()):
        nonnull = [value for value in values if value is not None]
        if not nonnull:
            output.append((group, None, None, None, 0, len(values)))
            continue
        sums = tuple(sum(value[axis] for value in nonnull) for axis in range(3))
        bad = [
            name
            for name, bits, value in zip(AXES, BITS, sums)
            if not -(1 << (bits - 1)) <= value < (1 << (bits - 1))
        ]
        overflows.extend((group, axis) for axis in bad)
        if not bad:
            output.append((group, sums[0], sums[1], sums[2], len(nonnull), len(values)))
    return Expected(tuple(output), tuple(overflows))


@dataclass(frozen=True)
class Plan:
    baseline: tuple[Input, ...]
    poison: tuple[Input, ...]
    extra: Input
    axis: str


def generate(rng: random.Random) -> Plan:
    rows: list[Input] = []
    groups = rng.randint(2, 4)
    for group in range(groups):
        parts = [
            rng.choice((1, 10, (1 << (bits - 2)) + rng.randint(1, 1000)))
            for bits in BITS
        ]
        vector = (parts[0], parts[1], parts[2])
        for _ in range(rng.randint(1, 3)):
            rows.extend(
                ((group, vector), (group, (-vector[0], -vector[1], -vector[2])))
            )
        anchor = (rng.randint(-3, 3), rng.randint(-3, 3), rng.randint(-1000, 1000))
        rows.extend(((group, anchor), (group, None)))
    rows.append((groups, None))
    rng.shuffle(rows)
    axis = rng.randrange(3)
    magnitude = rng.randint(3 * (1 << (BITS[axis] - 3)), (1 << (BITS[axis] - 1)) - 1000)
    value = [0, 0, 0]
    value[axis] = rng.choice((-1, 1)) * magnitude
    poison: Input = (0, (value[0], value[1], value[2]))
    return Plan(
        tuple(rows),
        (poison, poison),
        (0, (rng.randint(-3, 3), rng.randint(-3, 3), rng.randint(-1000, 1000))),
        AXES[axis],
    )
