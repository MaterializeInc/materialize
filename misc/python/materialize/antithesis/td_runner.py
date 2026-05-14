# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Generic `testdrive` runner for Antithesis Test Composer commands.

Most Tier 1 wrappers — anything where the existing `.td` corpus already
expresses the property we want under fault injection — should be able to
reduce to a 3-line command file:

    from materialize.antithesis.td_runner import run_file

    run_file("/opt/materialize/td/testdrive/foo.td",
             source="antithesis/foo/singleton_driver_foo")

This module owns the subprocess invocation, the tolerated-failure path, and
the SDK assertions that mark which outcome the runner reached.

Why two distinct `reachable` messages: Antithesis aggregates by message, and
we want the `td_runner saw tolerated failure` bucket to be visible in triage
even on timelines that ultimately succeed on retry. Keeping the success and
tolerated-failure messages distinct lets us tell those apart at a glance.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

from materialize.antithesis.sdk import reachable
from materialize.antithesis.testdrive_config import TestdriveConfig


def run_file(
    td_path: str | Path,
    *,
    source: str,
    config: TestdriveConfig | None = None,
    vars: Mapping[str, str] | None = None,
    no_reset: bool = True,
    tolerate_transient: bool = True,
    timeout_seconds: float = 600.0,
) -> None:
    """Run a single `.td` file via testdrive against the running SUT.

    `tolerate_transient=True` retries once on subprocess failure or timeout,
    after emitting a `reachable` assertion that records the tolerated failure.
    A second failure is allowed to propagate as a non-zero exit so Antithesis
    flags the timeline.

    `source` should match the Test Composer command path (relative under
    `/opt/antithesis/test/v1/`) so testdrive's `--source` flag attributes
    output correctly in triage.

    `vars` is forwarded as `--var=key=value` and is the right place for
    anything you need to override per-call (e.g. small data sizes for
    bounded-runtime variants).
    """

    config = config or TestdriveConfig.from_env()
    td_path = Path(td_path)

    cmd = config.base_command(no_reset=no_reset, source=source)
    if vars:
        for key, value in vars.items():
            cmd.append(f"--var={key}={value}")
    cmd.append(str(td_path))

    attempts = 2 if tolerate_transient else 1
    last_error: BaseException | None = None

    for attempt in range(attempts):
        try:
            subprocess.run(cmd, check=True, timeout=timeout_seconds)
            reachable(
                "td_runner completed .td file",
                {
                    "td_path": str(td_path),
                    "td_name": td_path.name,
                    "source": source,
                    "attempt": attempt,
                },
            )
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            last_error = e
            if attempt + 1 < attempts:
                # Tolerated failure path: expose it to triage but keep the
                # timeline alive so the retry has a chance.
                reachable(
                    "td_runner saw tolerated failure",
                    {
                        "td_path": str(td_path),
                        "td_name": td_path.name,
                        "source": source,
                        "attempt": attempt,
                        "error_class": type(e).__name__,
                        "returncode": getattr(e, "returncode", None),
                    },
                )

    assert last_error is not None
    raise last_error
