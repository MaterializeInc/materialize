# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import DEFAULT_CONFLUENT_PLATFORM_VERSION
from materialize.mzcompose.service import Service, ServiceConfig


class SchemaRegistry(Service):
    def __init__(
        self,
        name: str = "schema-registry",
        aliases: list[str] = [],
        image: str = "confluentinc/cp-schema-registry",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 8081,
        kafka_servers: list[tuple[str, str]] = [("kafka", "9092")],
        environment_extra: list[str] = [],
        depends_on_extra: list[str] = [],
        volumes: list[str] = [],
        platform: str | None = None,
    ) -> None:
        bootstrap_servers = ",".join(
            f"PLAINTEXT://{host}:{port}" for host, port in kafka_servers
        )
        environment = [
            # Under Docker, Kafka can be really slow, which means the default
            # Kafka connection timeout of 500ms is much too slow.
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            f"SCHEMA_REGISTRY_HOST_NAME={name}",
            f"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS={bootstrap_servers}",
            *environment_extra,
        ]
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": [port],
            "networks": {"default": {"aliases": aliases}},
            "environment": environment,
            "depends_on": {
                **{host: {"condition": "service_healthy"} for host, _ in kafka_servers},
                **{s: {"condition": "service_started"} for s in depends_on_extra},
            },
            "healthcheck": {
                "test": [
                    "CMD",
                    "curl",
                    # We provide credentials in case the schema registry is
                    # configured to require HTTP authentication, as there's
                    # no health check endpoint that's excluded from
                    # authentication requirements. The credentials are
                    # safely ignored if the schema registry is not
                    # configured to require them.
                    "-fu",
                    "materialize:sekurity",
                    "localhost:8081",
                ],
                "interval": "1s",
                "start_period": "120s",
            },
            "volumes": volumes,
        }
        if platform:
            config["platform"] = platform
        super().__init__(
            name=name,
            config=config,
        )
