# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import (
    DEFAULT_MZ_VOLUMES,
)
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class Clusterd(Service):
    def __init__(
        self,
        name: str = "clusterd",
        image: str | None = None,
        environment_extra: list[str] = [],
        memory: str | None = None,
        options: list[str] = [],
    ) -> None:
        environment = [
            "CLUSTERD_LOG_FILTER",
            "MZ_SOFT_ASSERTIONS=1",
            *environment_extra,
        ]

        command = []

        command += options

        config: ServiceConfig = {}

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "clusterd"

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "command": command,
                "ports": [2100, 2101, 6878],
                "environment": environment,
                "volumes": DEFAULT_MZ_VOLUMES,
            }
        )

        super().__init__(name=name, config=config)
