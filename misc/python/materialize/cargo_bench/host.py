# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Identification of the machine a cargo-bench run measured on.

Agents in one CI queue are not interchangeable for timing. The same pair of
bench binaries, measured on six agents, flagged a different benchmark on
each, by up to 25% in either direction, while repeated runs on one agent
agreed with each other. Recording the CPU next to the results lets a flagged
regression be checked against the hardware it was measured on.
"""

import os
import platform


def cpu_identity(cpuinfo: str) -> tuple[str | None, str | None]:
    """Return the model name and microcode revision of the first processor in `cpuinfo`, the text of `/proc/cpuinfo`.

    Either is `None` when the kernel does not report it.
    """
    model = None
    microcode = None
    for line in cpuinfo.splitlines():
        # A blank line ends the first processor's block.
        if not line.strip():
            break
        key, _, value = line.partition(":")
        key = key.strip()
        if key == "model name":
            model = value.strip()
        elif key == "microcode":
            microcode = value.strip()
    return model, microcode


def describe_host() -> str:
    """Return a one-line Markdown description of the agent, CPU, microcode, and kernel this process runs on."""
    try:
        with open("/proc/cpuinfo") as f:
            model, microcode = cpu_identity(f.read())
    except OSError:
        model, microcode = None, None
    name = os.getenv("BUILDKITE_AGENT_NAME") or platform.node()
    parts = [f"`{name}`", model or "unknown CPU"]
    if microcode:
        parts.append(f"microcode {microcode}")
    parts.append(f"kernel {platform.release()}")
    return ", ".join(parts)
