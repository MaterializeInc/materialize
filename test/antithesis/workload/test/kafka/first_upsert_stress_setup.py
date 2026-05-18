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

Creates the multi-partition upsert stress source so the standalone
`upsert-hammer-{i}` containers' continuous production has a Materialize
source consuming it. Idempotent — the hammer containers also ensure the
underlying topic exists, so this runs cleanly whether the hammer has
already started or not.

Lives in the kafka template directory because the upsert-stress group
uses the kafka template at runtime (it shares the kafka anytime safety
properties — `anytime_kafka_frontier_monotonic` and
`anytime_kafka_offset_known_not_below_committed`).
"""

from __future__ import annotations

import sys

import helper_logging
from helper_upsert_stress import ensure_upsert_stress_source

LOG = helper_logging.setup_logging("first.upsert_stress_setup")


def main() -> int:
    ensure_upsert_stress_source()
    LOG.info("upsert-stress setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
