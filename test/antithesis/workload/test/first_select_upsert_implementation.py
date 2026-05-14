#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis test command: pick v1 or v2 of the upsert continual feedback
operator at the start of each timeline.

The selection is made via `helper_random.random_u64()` (routes through the
Antithesis SDK for deterministic replay) and applied via `ALTER SYSTEM SET
enable_upsert_v2 = ...` against the `mz_system` internal port. Because this
script is a `first_*` Test Composer action it runs after `setup-complete`
but before any `parallel_driver_*` / `singleton_driver_*` creates a source,
so every source rendered in this timeline reads the chosen value.

Each branch records a `sometimes` assertion so Antithesis surfaces "v1
covered" and "v2 covered" as separate dashboard signals — if either ever
goes 0/N across the run, we've lost that arm of coverage.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_random
from helper_pg import execute_internal_retry

from antithesis.assertions import sometimes

LOG = helper_logging.setup_logging("first.select_upsert_implementation")


def main() -> int:
    # Low bit of a SDK-sourced u64 — under Antithesis this routes through the
    # SDK so timeline replay picks the same arm; outside Antithesis it falls
    # back to a stdlib-seeded RNG (see helper_random).
    enable_v2 = (helper_random.random_u64() & 1) == 1
    LOG.info("rolled enable_upsert_v2=%s for this timeline", enable_v2)

    # Set explicitly in both branches so the chosen value is part of the
    # timeline's recorded state, not implicit in the bootstrap default.
    if enable_v2:
        execute_internal_retry("ALTER SYSTEM SET enable_upsert_v2 = true")
        sometimes(True, "upsert continual feedback v2 enabled for timeline", {})
    else:
        execute_internal_retry("ALTER SYSTEM SET enable_upsert_v2 = false")
        sometimes(True, "upsert continual feedback v1 enabled for timeline", {})
    return 0


if __name__ == "__main__":
    sys.exit(main())
