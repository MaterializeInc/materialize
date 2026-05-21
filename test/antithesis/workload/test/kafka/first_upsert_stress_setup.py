#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis `first_*` setup for the INC-936 upsert-stress group.

Does two things, in order, because Antithesis runs exactly one `first_*`
per timeline (multiple `first_*` entries in a template are mutually
exclusive — Antithesis picks one):

1. Pin `enable_upsert_v2 = false` so every source created on this
   timeline uses the v1 continual-feedback operator. INC-936 reproduces
   against v1 specifically; rolling v1/v2 would dilute the v1-side
   signal. Must precede source creation so the stress source picks up
   the pinned value.
2. Create the multi-partition upsert stress source so the standalone
   `upsert-hammer-{i}` containers' continuous production has a
   Materialize source consuming it. Idempotent — the hammer containers
   also ensure the underlying topic exists, so this runs cleanly
   whether the hammer has already started or not.

Lives in the kafka template directory because the upsert-stress group
uses the kafka template at runtime (it shares the kafka anytime safety
properties — `anytime_kafka_frontier_monotonic` and
`anytime_kafka_offset_known_not_below_committed`).
"""

from __future__ import annotations

import sys

import helper_logging
from antithesis.assertions import sometimes
from helper_pg import execute_internal_retry
from helper_upsert_stress import ensure_upsert_stress_source

LOG = helper_logging.setup_logging("first.upsert_stress_setup")


def main() -> int:
    LOG.info("pinning enable_upsert_v2=false for this timeline")
    execute_internal_retry("ALTER SYSTEM SET enable_upsert_v2 = false")
    sometimes(True, "upsert continual feedback v1 pinned for timeline", {})

    ensure_upsert_stress_source()
    LOG.info("upsert-stress setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
