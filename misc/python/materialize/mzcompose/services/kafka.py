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
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class Kafka(Service):
    def __init__(
        self,
        name: str = "kafka",
        image: str = "confluentinc/cp-kafka",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        ports: list[str | int] | None = None,
        allow_host_ports: bool = False,
        auto_create_topics: bool = False,
        broker_id: int = 1,
        offsets_topic_replication_factor: int = 1,
        advertised_listeners: list[str] = [],
        environment: list[str] = [
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
            "KAFKA_MIN_INSYNC_REPLICAS=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_MESSAGE_MAX_BYTES=15728640",
            "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=100",
        ],
        environment_extra: list[str] = [],
        depends_on_extra: list[str] = [],
        volumes: list[str] = [],
        platform: str | None = None,
    ) -> None:
        if not advertised_listeners:
            advertised_listeners = [f"PLAINTEXT://{name}:9092"]
        environment = [
            *environment,
            f"KAFKA_ADVERTISED_LISTENERS={','.join(advertised_listeners)}",
            f"KAFKA_BROKER_ID={broker_id}",
            *environment_extra,
        ]
        if ports is None:
            ports = [l.split(":")[-1] for l in advertised_listeners]
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": ports,
            "allow_host_ports": allow_host_ports,
            "environment": [
                *environment,
                f"KAFKA_AUTO_CREATE_TOPICS_ENABLE={auto_create_topics}",
                f"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR={offsets_topic_replication_factor}",
            ],
            "depends_on": {
                "zookeeper": {"condition": "service_healthy"},
                **{s: {"condition": "service_started"} for s in depends_on_extra},
            },
            "healthcheck": {
                "test": ["CMD", "nc", "-z", "localhost", "9092"],
                "interval": "1s",
                "start_period": "120s",
            },
            "volumes": volumes,
        }
        if platform:
            config["platform"] = platform
        super().__init__(name=name, config=config)
