# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os
import random
import subprocess
from collections.abc import Mapping
from typing import Any

try:
    from antithesis.assertions import (  # pyright: ignore[reportMissingTypeStubs]
        always,
        reachable,
        sometimes,
        unreachable,
    )
except Exception:

    def always(condition: bool, message: str, details: Mapping[str, Any]) -> None:
        if not condition:
            raise AssertionError(f"{message}: {details}")

    def sometimes(condition: bool, message: str, details: Mapping[str, Any]) -> None:
        # Local fallback: do not enforce hit aggregation outside Antithesis.
        _ = condition, message, details

    def reachable(message: str, details: Mapping[str, Any]) -> None:
        _ = message, details

    def unreachable(message: str, details: Mapping[str, Any]) -> None:
        raise AssertionError(f"{message}: {details}")


try:
    from antithesis.lifecycle import (  # pyright: ignore[reportMissingTypeStubs]
        setup_complete,
    )
except Exception:

    def setup_complete(details: Mapping[str, Any]) -> None:
        print(f"setup_complete: {dict(details)}")


try:
    from antithesis.random import (  # pyright: ignore[reportMissingTypeStubs]
        get_random,
        random_choice,
    )
except Exception:

    def get_random() -> int:
        return random.getrandbits(64)

    # Match the upstream antithesis.random signature, which is loosely typed
    # as `(things: list[Any]) -> Any`. Tightening to a generic `Sequence[T]`
    # here would make pyright complain about the signature drift between the
    # real SDK and our fallback.
    def random_choice(things: list[Any]) -> Any:
        return random.choice(things)


def random_int(max_exclusive: int) -> int:
    assert max_exclusive > 0
    return get_random() % max_exclusive


def stop_faults(duration_seconds: int) -> bool:
    """Request an Antithesis quiet period, if running inside Antithesis."""

    stop_faults_bin = os.environ.get("ANTITHESIS_STOP_FAULTS")
    if not stop_faults_bin:
        return False

    subprocess.run([stop_faults_bin, str(duration_seconds)], check=True)
    return True
