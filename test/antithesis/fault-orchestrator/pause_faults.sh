#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Drive Antithesis fault windows globally.
#
# Antithesis injects faults into the system continuously by default.
# Calling `ANTITHESIS_STOP_FAULTS <seconds>` requests a quiet window —
# Antithesis pauses fault injection for that many seconds. The Antithesis
# engagement team's recommendation: drive these quiet windows from a
# single dedicated container, not per-driver, otherwise overlapping
# per-driver requests keep the system in a quiet state most of the time
# and we never actually fault.
#
# This script alternates faults-OFF (quiet) and faults-ON (active)
# windows at randomized intervals so each timeline sees a different
# cadence. Adapted from the Antithesis hands-on tutorial:
#   https://github.com/antithesishq/hands-on-tutorial-1/blob/main/python/antithesis/pause_faults.sh
#
# Outside Antithesis (snouty local validate) `ANTITHESIS_STOP_FAULTS` is
# unset; the script exits immediately so the rest of the compose works.

set -euo pipefail

if [[ -z "${ANTITHESIS_STOP_FAULTS:-}" ]]; then
    echo "ANTITHESIS_STOP_FAULTS not set; fault-orchestrator exiting (no-op)"
    exit 0
fi

# Tunable via the service `environment:` block. Defaults sized so that:
#   * MAX_ON is comfortably shorter than any driver's CATCHUP_TIMEOUT_S
#     (smallest is 60s in parallel_driver_upsert_latest_value) — a
#     driver's catchup window can always span at least one full quiet
#     period.
#   * MIN_OFF is long enough for materialized to commit a few timestamps
#     and for sources to advance offset_committed past the most recent
#     batch of produced offsets.
#   * START_DELAY gives setup-complete + bootstrap a window of un-faulted
#     time before the alternation begins.
START_DELAY="${START_DELAY:-30}"
MIN_ON="${MIN_ON:-20}"
MAX_ON="${MAX_ON:-40}"
MIN_OFF="${MIN_OFF:-20}"
MAX_OFF="${MAX_OFF:-40}"

echo "fault-orchestrator: ON ${MIN_ON}-${MAX_ON}s / OFF ${MIN_OFF}-${MAX_OFF}s, initial pause ${START_DELAY}s"

# Initial quiet window so the rest of the stack reaches steady state
# before Antithesis starts faulting. Antithesis may or may not honour
# this depending on when fault injection begins relative to setup-
# complete; either way the local sleep gives drivers a clean start.
"${ANTITHESIS_STOP_FAULTS}" "${START_DELAY}"
sleep "${START_DELAY}"

while true; do
    # Re-seed $RANDOM from /dev/urandom so successive iterations don't
    # repeat the same on/off period (the shell's RANDOM is a 16-bit LCG;
    # without reseeding it can produce predictable sequences).
    RANDOM=$(od -An -N2 -tu2 /dev/urandom | tr -d ' ')
    ON_PERIOD=$((MIN_ON + (RANDOM % (MAX_ON - MIN_ON + 1))))
    OFF_PERIOD=$((MIN_OFF + (RANDOM % (MAX_OFF - MIN_OFF + 1))))

    echo "fault-orchestrator: faults OFF for ${OFF_PERIOD}s"
    "${ANTITHESIS_STOP_FAULTS}" "${OFF_PERIOD}"
    sleep "${OFF_PERIOD}"

    echo "fault-orchestrator: faults ON for ${ON_PERIOD}s"
    sleep "${ON_PERIOD}"
done
