# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Wrapper around the Antithesis ANTITHESIS_STOP_FAULTS binary.

Outside Antithesis (e.g. snouty local validate), the env var is unset and this
becomes a no-op so the workload still runs end-to-end.
"""

from __future__ import annotations

import logging
import os
import subprocess

LOG = logging.getLogger("antithesis.helper_quiet")


def request_quiet_period(seconds: int) -> bool:
    """Request that Antithesis pause all faults for `seconds`.

    Returns True if the request was issued, False if not in Antithesis. Either
    way callers must still poll for the system to stabilize — the binary
    returns immediately and the actual quiet window unfolds asynchronously.
    """
    binary = os.environ.get("ANTITHESIS_STOP_FAULTS")
    if not binary:
        LOG.info("ANTITHESIS_STOP_FAULTS not set; skipping quiet-period request")
        return False
    LOG.info("requesting %ds quiet period via %s", seconds, binary)
    subprocess.run([binary, str(seconds)], check=False)
    return True
