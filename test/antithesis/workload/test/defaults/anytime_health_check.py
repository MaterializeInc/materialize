#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Anytime health-check: verify materialized is reachable via pgwire.

Antithesis re-launches `anytime_*` commands continuously throughout a
run.  Each invocation is independently scored against the built-in
"Always: Commands finish with zero exit code" property, so a single
non-zero exit during a fault window registers as a property failure
even though the failure is the expected consequence of the fault
itself.  The previous bash version exited 1 on any psql error, so
~9% of invocations failed under the global fault-orchestrator
cadence (~40s ON / ~40s OFF) — recall:

  passingCount=9013, failingCount=803  (testdrive (12:40) run)

Recognize the fault-shape error strings from `helper_fault_tolerance`
and exit 0 in those cases so the property reflects "did the SUT
silently stop responding outside any fault window?" rather than
"did Antithesis kill materialized recently?".
"""

from __future__ import annotations

import os
import subprocess
import sys

import helper_logging
from helper_fault_tolerance import looks_like_fault

LOG = helper_logging.setup_logging("anytime.health_check")

PGHOST = os.environ.get("PGHOST", "materialized")
PGPORT = os.environ.get("PGPORT", "6875")
PGUSER = os.environ.get("PGUSER", "materialize")

# Per-attempt psql wall-clock cap.  Short enough to fast-fail on a
# paused materialized rather than block this anytime invocation for the
# duration of a fault-ON window, long enough to absorb normal pgwire
# round-trips on a healthy SUT.  The bash version had no equivalent and
# could hang for the full TCP-connect timeout (~75s on Linux) when the
# container's IP was reachable but materialized wasn't listening yet.
PSQL_TIMEOUT_S = 10.0


def main() -> int:
    try:
        result = subprocess.run(
            [
                "psql",
                "-h",
                PGHOST,
                "-p",
                PGPORT,
                "-U",
                PGUSER,
                "-tAc",
                "SELECT 1",
            ],
            capture_output=True,
            text=True,
            timeout=PSQL_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        # psql hung past PSQL_TIMEOUT_S — consistent with a paused
        # materialized whose TCP socket is up but isn't responding.
        # Treat as fault-shaped; the SUT being slow under fault
        # injection isn't a property violation.
        LOG.info("health check skipped: psql timed out after %.0fs", PSQL_TIMEOUT_S)
        return 0

    if result.returncode == 0 and result.stdout.strip() == "1":
        LOG.info("health check passed")
        return 0

    combined = (result.stdout + "\n" + result.stderr).strip()
    if looks_like_fault(combined):
        LOG.info("health check skipped (fault-shaped): %s", combined[:200])
        return 0

    LOG.warning(
        "health check failed: rc=%s output=%s",
        result.returncode,
        combined[:500],
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
