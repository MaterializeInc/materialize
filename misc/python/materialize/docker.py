# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tools to help interacting with docker
"""

from typing import NamedTuple

from materialize import spawn

RECOMMENDED_MIN_MEM = 8 * 1024 ** 3  # 8GiB
RECOMMENDED_MIN_CPUS = 2


class DockerInfo(NamedTuple):
    "Total number of bytes available to docker"
    mem_total: int
    "Total number of cpus available to docker"
    ncpus: int


def resource_limits() -> DockerInfo:
    args = ["docker", "system", "info", "--format", "{{.MemTotal}} {{.NCPU}}"]
    memtotal, ncpu = spawn.capture(args, unicode=True).split()
    return DockerInfo(int(memtotal), int(ncpu))
