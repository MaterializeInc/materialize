# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Randomness drawn from Antithesis, so every choice a driver makes is explorable."""

import random

from antithesis.random import (  # pyright: ignore[reportMissingModuleSource]
    get_random,
)


class AntithesisRandom(random.Random):
    """A `random.Random` whose entropy comes from the Antithesis SDK.

    Every draw calls into the SDK, so Antithesis can branch on any decision the
    workload makes. Outside Antithesis the SDK falls back to system randomness.
    Seeding is a no-op: reproducibility comes from Antithesis replay, not from a
    seed.
    """

    def random(self) -> float:
        return (get_random() >> 11) * (1.0 / (1 << 53))

    def getrandbits(self, k: int) -> int:
        if k <= 0:
            return 0
        bits = 0
        produced = 0
        while produced < k:
            bits = (bits << 64) | get_random()
            produced += 64
        return bits >> (produced - k)

    def seed(self, a: object = None, version: int = 2) -> None:
        pass

    def getstate(self) -> object:
        raise NotImplementedError("AntithesisRandom has no reproducible state")

    def setstate(self, state: object) -> None:
        raise NotImplementedError("AntithesisRandom has no reproducible state")


rng = AntithesisRandom()
