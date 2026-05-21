# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.spawn` for the Antithesis workload image.

The real module is `from materialize import spawn`-imported at module
top by `materialize.checks.actions` (so its `UseOptimizedProfile` /
`GitResetHard` actions can hold runv references) and by
`materialize.mz_version` (which we already bundle).  Both callers only
*invoke* `spawn.runv` from Action.execute paths that the Antithesis
driver never triggers — so a stub with the runv signature is enough to
keep top-level imports happy.

Shipping a stub keeps the workload image free of the `colored` pip
dep that the real `spawn` -> `ui` chain pulls in transitively.
"""

from __future__ import annotations

import subprocess
from typing import Any


def runv(args: Any, *_args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
    """Stub matching `materialize.spawn.runv` signature.

    Called by `UseOptimizedProfile.execute` / `GitResetHard.execute`
    in `materialize.checks.actions`, neither of which the Antithesis
    driver routes through.  The driver constructs Check objects to
    read `.input` from their Testdrive wrappers; the Action.execute
    layer never runs here.  If something on this path *did* call
    runv, the error message lands in the driver's stack trace and
    tells the human what to wire up next.
    """
    raise RuntimeError(
        "materialize.spawn.runv is stubbed in the Antithesis workload image; "
        "the Action.execute() path is not driven from here."
    )


def capture(args: Any, *_args: Any, **kwargs: Any) -> str:
    """Stub matching `materialize.spawn.capture`. See runv for context."""
    raise RuntimeError(
        "materialize.spawn.capture is stubbed in the Antithesis workload image."
    )
