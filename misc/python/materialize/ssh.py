# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SSH utilities."""

import subprocess
from typing import List, Optional

from materialize import spawn


def runv(
    args: List[str],
    username: str,
    host: str,
    check_host_key: bool = False,
    identity_file: Optional[str] = None,
) -> subprocess.CompletedProcess:
    initial_args = ["ssh"]
    if not check_host_key:
        initial_args += ["-o", "StrictHostKeyChecking off"]
    if identity_file:
        initial_args += ["-i", identity_file, "-o", "IdentitiesOnly yes"]
    initial_args += [f"{username}@{host}"]
    return spawn.runv(initial_args + args)
