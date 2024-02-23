# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.service import Service

FIVETRAN_TESTER_VERSION = "024.0222.002"


class FivetranDestinationTester(Service):
    def __init__(
        self,
        destination_host: str,
        destination_port: int,
        environment_extra: list[str] = [],
        volumes_extra: list[str] = [],
    ) -> None:
        environment = [
            f"GRPC_HOSTNAME={destination_host}",
            *environment_extra,
        ]
        command = [f"--port={destination_port}"]
        super().__init__(
            name="fivetran-destination-tester",
            config={
                "image": f"fivetrandocker/sdk-destination-tester:{FIVETRAN_TESTER_VERSION}",
                "command": command,
                "environment": environment,
                "volumes": volumes_extra,
            },
        )
