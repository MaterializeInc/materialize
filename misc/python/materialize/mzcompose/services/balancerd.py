# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDependency,
)


class Balancerd(Service):
    def __init__(
        self,
        name: str = "balancerd",
        mzbuild: str = "balancerd",
        command: list[str] | None = None,
        volumes: list[str] = [],
        depends_on: list[str] = [],
    ) -> None:
        if command is None:
            command = [
                "service",
                "--pgwire-listen-addr=0.0.0.0:6875",
                "--https-listen-addr=0.0.0.0:6876",
                "--internal-http-listen-addr=0.0.0.0:6878",
                "--static-resolver-addr=materialized:6875",
                "--https-resolver-template='materialized:6876'",
            ]

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }
        config: ServiceConfig = {
            "mzbuild": mzbuild,
            "command": command,
            "ports": [6875, 6876, 6877, 6878],
            "volumes": volumes,
            "depends_on": depends_graph,
        }
        super().__init__(
            name=name,
            config=config,
        )
