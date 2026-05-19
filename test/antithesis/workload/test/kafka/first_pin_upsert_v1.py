#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis test command: pin the upsert continual-feedback operator
to v1 (the existing operator) for this timeline.

Sibling of `first_select_upsert_implementation.py`, which rolls v1/v2
from SDK entropy.  This variant always selects v1 — used by the
`upsert-stress` group, where the goal is to reproduce INC-936
("invalid upsert state" panic) against the existing operator
specifically.  Rolling v2 here would split the timeline budget across
two operator implementations and dilute the v1-side reproduction
signal we're after.

Idempotent via `ALTER SYSTEM SET`; runs after `setup-complete` but
before any `parallel_driver_*` / `singleton_driver_*` creates a
source, so every source in the timeline reads the pinned value.
"""

from __future__ import annotations

import sys

import helper_logging
from helper_pg import execute_internal_retry

from antithesis.assertions import sometimes

LOG = helper_logging.setup_logging("first.pin_upsert_v1")


def main() -> int:
    LOG.info("pinning enable_upsert_v2=false for this timeline")
    execute_internal_retry("ALTER SYSTEM SET enable_upsert_v2 = false")
    sometimes(True, "upsert continual feedback v1 pinned for timeline", {})
    return 0


if __name__ == "__main__":
    sys.exit(main())
