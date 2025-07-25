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
        https_resolver_template: str | None = None,
        frontegg_resolver_template: str | None = None,
        static_resolver_addr: str | None = None,
    ) -> None:
        if command is None:
            command = [
                "service",
                "--pgwire-listen-addr=0.0.0.0:6575",
                "--https-listen-addr=0.0.0.0:6576",
                "--internal-http-listen-addr=0.0.0.0:6578",
                f"--static-resolver-addr={static_resolver_addr or 'materialized:6875'}",
                f"--https-resolver-template={https_resolver_template or 'materialized:6876'}",
            ]
        else:
            if static_resolver_addr is not None:
                command.append(f"--static-resolver-addr={static_resolver_addr}")
            if https_resolver_template is not None:
                command.append(f"--https-resolver-template={https_resolver_template}")
        if frontegg_resolver_template is not None:
            command.append(
                f"--frontegg-reesolver-template={frontegg_resolver_template}"
            )

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }
        config: ServiceConfig = {
            "mzbuild": mzbuild,
            "command": command,
            "ports": [6575, 6576, 6578],
            "volumes": volumes,
            "depends_on": depends_graph,
        }
        super().__init__(
            name=name,
            config=config,
        )
