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


def exists(resource: str, context: str = "kind-kind") -> None:
    _exists(resource, True, context)


def not_exists(resource: str, context: str = "kind-kind") -> None:
    _exists(resource, False, context)


def _exists(resource: str, should_exist: bool, context: str = "kind-kind") -> None:
    cmd = ["kubectl", "get", "--output", "name", resource, "--context", context]
    ui.progress(f'running {" ".join(cmd)} ... ')

    try:
        result = subprocess.run(cmd, capture_output=True, encoding="ascii")
        result.check_returncode()
        if should_exist:
            ui.progress("success!", finish=True)
        else:
            raise UIError(f"{resource} exists, but expected it not to")
    except subprocess.CalledProcessError as e:
        # A bit gross, but it should be safe enough in practice.
        if "(NotFound)" in e.stderr:
            if should_exist:
                ui.progress("error!", finish=True)
                raise UIError(f"{resource} does not exist, but expected it to")
            else:
                ui.progress("success!", finish=True)
        else:
            ui.progress(finish=True)
            raise UIError(f"kubectl failed: {e}")
