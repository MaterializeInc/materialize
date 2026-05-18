# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Deterministic randomness for Antithesis drivers.

All driver randomness must go through the Antithesis SDK so timelines replay
deterministically. Outside Antithesis we fall back to the stdlib `random` with a
fixed-but-arbitrary seed per process so local runs are not flaky.
"""

from __future__ import annotations

import os
import random as _stdlib_random
from collections.abc import Sequence
from typing import TypeVar

try:
    from antithesis import random as _ar

    _ANTITHESIS = True
except ImportError:
    _ANTITHESIS = False

T = TypeVar("T")

# A stable per-process seed so local snouty validate runs are deterministic
# within one process but pick a different sequence per process invocation.
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
    # Use 16 bits of entropy to avoid floating-point quirks under replay.
    return (random_u64() & 0xFFFF) < int(true_prob * 0x10000)
