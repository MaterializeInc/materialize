# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The random choices a rule makes.

Components draw through `Draw` so they do not depend on hegel: the state
machine passes draws backed by hegel's test case, which Antithesis controls,
and unit tests pass `SeededDraw`.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import Protocol, TypeVar

T = TypeVar("T")


class Draw(Protocol):
    def integer(self, lo: int, hi: int) -> int:
        """An integer in `[lo, hi]`."""
        ...

    def boolean(self, p: float = 0.5) -> bool:
        """True with probability `p`."""
        ...

    def choice(self, options: Sequence[T]) -> T: ...


class SeededDraw:
    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)

    def integer(self, lo: int, hi: int) -> int:
        return self.rng.randint(lo, hi)

    def boolean(self, p: float = 0.5) -> bool:
        return self.rng.random() < p

    def choice(self, options: Sequence[T]) -> T:
        return options[self.rng.randrange(len(options))]
