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
)


class Localstack(Service):
    def __init__(
        self,
        name: str = "localstack",
        image: str = "localstack/localstack:4.12.0",
        port: int = 4566,
        environment: list[str] = ["LOCALSTACK_HOST=localstack"],
        volumes: list[str] = ["/var/run/docker.sock:/var/run/docker.sock"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "init": True,
                "ports": [port],
                "environment": environment,
                "volumes": volumes,
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:4566/_localstack/health"],
                    "interval": "1s",
                    "start_period": "120s",
                },
            },
        )
