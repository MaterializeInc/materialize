# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import subprocess
from typing import Optional


def print_pods(
    context: str = "kind-cloudtest",
    label: Optional[str] = None,
) -> None:
    cmd = [
        "kubectl",
        "get",
        "pods",
        "--context",
        context,
    ]

    if label is not None:
        cmd.extend(["--selector", label])

    try:
        print("Pods are:")
        subprocess.run(cmd)
    except subprocess.CalledProcessError as e:
        print(e, e.output)
