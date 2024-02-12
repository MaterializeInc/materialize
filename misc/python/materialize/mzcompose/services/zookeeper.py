# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import (
    DEFAULT_CONFLUENT_PLATFORM_VERSION,
)
from materialize.mzcompose.service import Service, ServiceConfig


class Zookeeper(Service):
    def __init__(
        self,
        name: str = "zookeeper",
        image: str = "confluentinc/cp-zookeeper",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 2181,
        volumes: list[str] = [],
        environment: list[str] = ["ZOOKEEPER_CLIENT_PORT=2181"],
        platform: str | None = None,
    ) -> None:
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": [port],
            "volumes": volumes,
            "environment": environment,
            "healthcheck": {
                "test": ["CMD", "nc", "-z", "localhost", "2181"],
                "interval": "1s",
                "start_period": "120s",
            },
        }
        if platform:
            config["platform"] = platform
        super().__init__(
            name=name,
            config=config,
        )
