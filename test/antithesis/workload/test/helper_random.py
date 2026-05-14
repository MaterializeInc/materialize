# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis randomness primitives for drivers.

Two layers:

  * Free functions (`random_u64`, `random_int`, `random_bool`, `random_choice`,
    `random_float`) for direct use in driver code. Each call draws fresh
    entropy from the Antithesis SDK so different timelines see different
    values at the same call site — that's how the fuzzer drives coverage.

  * `AntithesisRandom`, a `random.Random` subclass that routes every
    `getrandbits()` and `random()` call through the SDK. Use it when
    handing an rng to code that expects a `random.Random` (notably
    `materialize.parallel_workload`'s `Worker`/`Action`). Seeding a stdlib
    `random.Random` from a single SDK draw and then making every
    subsequent decision deterministic locks the fuzzer out of every
    branch in that subtree; this class avoids that.

Outside Antithesis (e.g. snouty local validate) the SDK is unavailable;
the helpers and the subclass fall back to a stdlib `Random` seeded from
`os.urandom` so local runs are non-deterministic but functional.
"""

from __future__ import annotations

import os
import random as _stdlib_random
from collections.abc import Sequence
from typing import Any, TypeVar

try:
    from antithesis import random as _ar

    _ANTITHESIS = True
except ImportError:
    _ANTITHESIS = False

T = TypeVar("T")

# Fallback rng for non-Antithesis runs. Seeded once at import time from
# the OS entropy pool so each process picks a different sequence.
_FALLBACK = _stdlib_random.Random(int.from_bytes(os.urandom(8), "little"))


def random_u64() -> int:
    if _ANTITHESIS:
        return _ar.get_random()
    return _FALLBACK.getrandbits(64)


def random_choice(seq: Sequence[T]) -> T:
    if not seq:
        raise ValueError("random_choice on empty sequence")
    if _ANTITHESIS:
        return _ar.random_choice(list(seq))
    return _FALLBACK.choice(seq)


def random_int(low: int, high: int) -> int:
    """Inclusive on both ends."""
    if low > high:
        raise ValueError("low > high")
    span = high - low + 1
    return low + (random_u64() % span)


def random_bool(true_prob: float) -> bool:
    if not 0.0 <= true_prob <= 1.0:
        raise ValueError("true_prob out of range")
    # 16 bits of entropy avoids floating-point quirks under replay.
    return (random_u64() & 0xFFFF) < int(true_prob * 0x10000)


def random_float(low: float, high: float) -> float:
    """Uniform draw from [low, high). Useful for swarm parameters where
    each driver invocation should pick its own probability/weight value
    so different timelines explore different workload mixes."""
    if low > high:
        raise ValueError("low > high")
    # 53 bits is the precision of a Python float's mantissa; matches what
    # stdlib `random.random()` returns.
    unit = random_u64() >> 11
    fraction = unit / (1 << 53)
    return low + fraction * (high - low)


class AntithesisRandom(_stdlib_random.Random):
    """A `random.Random` whose every draw comes from the Antithesis SDK.

    The CPython `random.Random` API routes `choice`, `randint`,
    `randrange`, `sample`, `shuffle`, etc. through `getrandbits()`, and
    `random()` is its only floating-point primitive. Overriding both
    here means anything handed an `AntithesisRandom` exercises Antithesis
    entropy at every decision point, not just once per seed.

    Outside Antithesis we delegate to the module-level `_FALLBACK` so
    local runs still produce values; instances share that fallback
    rather than each carrying their own state.

    `seed()` is intentionally a no-op: a Mersenne-Twister-style seed
    isn't meaningful when entropy is supplied per-draw. `getstate` /
    `setstate` raise because the SDK's internal state isn't observable
    or restorable.
    """

    def random(self) -> float:
        # Match stdlib `Random.random()` width: top 53 bits of a u64.
        return (random_u64() >> 11) / (1 << 53)

    def getrandbits(self, k: int) -> int:
        if k <= 0:
            raise ValueError("number of bits must be greater than zero")
        # Pull 64-bit chunks until we have at least k bits, then shift the
        # surplus off the bottom so the result is in [0, 2**k).
        nchunks = (k + 63) // 64
        bits = 0
        for _ in range(nchunks):
            bits = (bits << 64) | random_u64()
        return bits >> (nchunks * 64 - k)

    def seed(self, *args: Any, **kwargs: Any) -> None:
        # Entropy comes from the SDK per call; nothing to seed.
        return None

    def getstate(self) -> Any:
        raise NotImplementedError(
            "AntithesisRandom has no snapshottable state; the SDK owns it"
        )

    def setstate(self, state: Any) -> None:
        raise NotImplementedError(
            "AntithesisRandom has no restorable state; the SDK owns it"
        )
