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
    ServiceDependency,
)

# Cluster ID used to format the KRaft metadata log on first boot. Must be a
# 22-character URL-safe base64 string (16 raw bytes). Decodes to the ASCII
# bytes of "MaterializeKafka".
DEFAULT_KAFKA_CLUSTER_ID = "TWF0ZXJpYWxpemVLYWZrYQ"


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
        listeners: list[str] | None = None,
        environment: list[str] | None = None,
        environment_extra: list[str] = [],
        depends_on_extra: list[str] = [],
        volumes: list[str] = [],
        platform: str | None = None,
        use_zookeeper: bool = False,
        controller_quorum_voters: str | None = None,
        controller_port: int = 9093,
        cluster_id: str = DEFAULT_KAFKA_CLUSTER_ID,
    ) -> None:
        if not advertised_listeners:
            advertised_listeners = [f"PLAINTEXT://{name}:9092"]

        if environment is None:
            environment = [
                "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
                "KAFKA_MIN_INSYNC_REPLICAS=1",
                "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "KAFKA_MESSAGE_MAX_BYTES=15728640",
                "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=100",
            ]
            if use_zookeeper:
                environment = [
                    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
                    *environment,
                ]

        if use_zookeeper:
            mode_environment = [
                f"KAFKA_ADVERTISED_LISTENERS={','.join(advertised_listeners)}",
                f"KAFKA_BROKER_ID={broker_id}",
            ]
            depends_on: dict[str, ServiceDependency] = {
                "zookeeper": ServiceDependency(condition="service_started"),
            }
        else:
            if listeners is None:
                listeners = []
                for adv in advertised_listeners:
                    proto, _, hostport = adv.partition("://")
                    _, _, port = hostport.partition(":")
                    listeners.append(f"{proto}://0.0.0.0:{port}")
                listeners.append(f"CONTROLLER://0.0.0.0:{controller_port}")
            if controller_quorum_voters is None:
                controller_quorum_voters = f"{broker_id}@{name}:{controller_port}"
            mode_environment = [
                f"KAFKA_NODE_ID={broker_id}",
                "KAFKA_PROCESS_ROLES=broker,controller",
                f"KAFKA_CONTROLLER_QUORUM_VOTERS={controller_quorum_voters}",
                f"KAFKA_LISTENERS={','.join(listeners)}",
                f"KAFKA_ADVERTISED_LISTENERS={','.join(advertised_listeners)}",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                f"CLUSTER_ID={cluster_id}",
            ]
            depends_on = {}

        environment = [
            *environment,
            *mode_environment,
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
                **depends_on,
                **{
                    s: ServiceDependency(condition="service_started")
                    for s in depends_on_extra
                },
            },
            "healthcheck": {
                # cp-kafka 8.x dropped `nc`, so use bash's built-in /dev/tcp
                # to probe the broker port without any extra dependencies.
                "test": [
                    "CMD-SHELL",
                    "bash -c 'exec 3<>/dev/tcp/localhost/9092'",
                ],
                "interval": "1s",
                "start_period": "120s",
            },
            "volumes": volumes,
        }
        if platform:
            config["platform"] = platform
        super().__init__(name=name, config=config)
