# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import MZ_ROOT
from materialize.mzcompose import (
    DEFAULT_CRDB_ENVIRONMENT,
    loader,
)
from materialize.mzcompose.service import (
    Service,
    ServiceHealthcheck,
)


class Cockroach(Service):
    DEFAULT_COCKROACH_TAG = "v23.1.11"

    def __init__(
        self,
        name: str = "cockroach",
        aliases: list[str] = ["cockroach"],
        image: str | None = None,
        command: list[str] | None = None,
        setup_materialize: bool = True,
        in_memory: bool = False,
        healthcheck: ServiceHealthcheck | None = None,
        # Workaround for #19809, should be "no" otherwise
        restart: str = "on-failure:5",
    ):
        volumes = []

        if image is None:
            image = f"cockroachdb/cockroach:{Cockroach.DEFAULT_COCKROACH_TAG}"

        if command is None:
            command = ["start-single-node", "--insecure"]

        if setup_materialize:
            path = os.path.relpath(
                MZ_ROOT / "misc" / "cockroach" / "setup_materialize.sql",
                loader.composition_path,
            )
            volumes += [f"{path}:/docker-entrypoint-initdb.d/setup_materialize.sql"]

        if in_memory:
            command.append("--store=type=mem,size=2G")

        if healthcheck is None:
            healthcheck = {
                # init_success is a file created by the Cockroach container entrypoint
                "test": "[ -f init_success ] && curl --fail 'http://localhost:8080/health?ready=1'",
                "interval": "1s",
                "start_period": "30s",
            }

        super().__init__(
            name=name,
            config={
                "image": image,
                "networks": {"default": {"aliases": aliases}},
                "ports": [26257],
                "command": command,
                "volumes": volumes,
                "init": True,
                "healthcheck": healthcheck,
                "restart": restart,
                "environment": DEFAULT_CRDB_ENVIRONMENT,
            },
        )
