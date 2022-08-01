# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import subprocess

from materialize import ui
from materialize.ui import UIError


def wait(
    condition: str, resource: str, timeout_secs: int = 300, context: str = "kind-kind"
) -> None:
    cmd = [
        "kubectl",
        "wait",
        "--for",
        condition,
        resource,
        "--timeout",
        f"{timeout_secs}s",
        "--context",
        context,
    ]
    ui.progress(f'waiting for {" ".join(cmd)} ... ')

    error = None
    for remaining in ui.timeout_loop(timeout_secs, tick=0.1):
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            if "condition met" in output.decode("ascii"):
                ui.progress("success!", finish=True)
                return
        except subprocess.CalledProcessError as e:
            error = e

    ui.progress(finish=True)
    raise UIError(f"kubectl wait never returned 'condition met': {error}")
