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
    Service,
)


class SchemaRegistry(Service):
    def __init__(
        self,
        name: str = "schema-registry",
        image: str = "confluentinc/cp-schema-registry",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 8081,
        kafka_servers: list[tuple[str, str]] = [("kafka", "9092")],
        bootstrap_server_type: str = "PLAINTEXT",
        environment: list[str] = [
            # NOTE(guswynn): under docker, kafka *can* be really slow, which means
            # the default of 500ms won't work, so we give it PLENTY of time
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
        ],
        depends_on_extra: list[str] = [],
        volumes: list[str] = [],
    ) -> None:
        bootstrap_servers = ",".join(
            f"{bootstrap_server_type}://{kafka}:{port}" for kafka, port in kafka_servers
        )
        environment = [
            *environment,
            f"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS={bootstrap_servers}",
        ]
        super().__init__(
            name=name,
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "environment": environment,
                "depends_on": {
                    **{
                        host: {"condition": "service_healthy"}
                        for host, _ in kafka_servers
                    },
                    **{s: {"condition": "service_started"} for s in depends_on_extra},
                },
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:8081"],
                    "interval": "1s",
                    "start_period": "120s",
                },
                "volumes": volumes,
            },
        )
