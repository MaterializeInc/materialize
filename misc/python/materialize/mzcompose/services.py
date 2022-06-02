# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import random
from typing import Dict, List, Optional, Tuple, Union

from materialize.mzcompose import Service, ServiceConfig

DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.0.3"
DEFAULT_DEBEZIUM_VERSION = "1.9"
LINT_DEBEZIUM_VERSIONS = ["1.4", "1.5", "1.6"]

DEFAULT_MZ_VOLUMES = [
    "mzdata:/mzdata",
    "pgdata:/var/lib/postgresql",
    "mydata:/var/lib/mysql-files",
    "tmp:/share/tmp",
]


class Materialized(Service):
    def __init__(
        self,
        name: str = "materialized",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        port: Union[int, str] = 6875,
        extra_ports: List[int] = [],
        memory: Optional[str] = None,
        data_directory: str = "/mzdata",
        timestamp_frequency: str = "1s",
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        environment_extra: Optional[List[str]] = None,
        forward_aws_credentials: bool = True,
        volumes: Optional[List[str]] = None,
        volumes_extra: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> None:
        if environment is None:
            environment = [
                "MZ_SOFT_ASSERTIONS=1",
                # Please think twice before forwarding additional environment
                # variables from the host, as it's easy to write tests that are
                # then accidentally dependent on the state of the host machine.
                #
                # To dynamically change the environment during a workflow run,
                # use Composition.override.
                "MZ_LOG_FILTER",
                "STORAGED_LOG_FILTER",
                "COMPUTED_LOG_FILTER",
            ]

        if forward_aws_credentials:
            environment += [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ]

        if environment_extra:
            environment.extend(environment_extra)

        if volumes is None:
            volumes = [*DEFAULT_MZ_VOLUMES]
        if volumes_extra:
            volumes.extend(volumes_extra)

        guest_port = port
        if isinstance(port, str) and ":" in port:
            guest_port = port.split(":")[1]

        command_list = [
            f"--data-directory={data_directory}",
            f"--listen-addr 0.0.0.0:{guest_port}",
            "--unsafe-mode",
            f"--timestamp-frequency {timestamp_frequency}",
        ]

        if options:
            if isinstance(options, str):
                command_list.append(options)
            else:
                command_list.extend(options)

        config: ServiceConfig = (
            {"image": image} if image else {"mzbuild": "materialized"}
        )

        if hostname:
            config["hostname"] = hostname

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "depends_on": depends_on or [],
                "command": " ".join(command_list),
                "ports": [port, 5432, *extra_ports],
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Computed(Service):
    def __init__(
        self,
        name: str = "computed",
        peers: Optional[List[str]] = [],
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        ports: List[int] = [2100, 2102],
        memory: Optional[str] = None,
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        workers: Optional[int] = None,
    ) -> None:
        if environment is None:
            environment = [
                "COMPUTED_LOG_FILTER",
                "MZ_SOFT_ASSERTIONS=1",
            ]

        if volumes is None:
            # We currently give computed access to /tmp so that it can load CSV files
            # but this requirement is expected to go away in the future.
            volumes = DEFAULT_MZ_VOLUMES

        config: ServiceConfig = {"image": image} if image else {"mzbuild": "computed"}

        if hostname:
            config["hostname"] = hostname

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        command_list = []
        if options:
            if isinstance(options, str):
                command_list.append(options)
            else:
                command_list.extend(options)

        if workers:
            command_list.append(f"--workers {workers}")

        if peers:
            command_list.append(f"--process {peers.index(name)}")
            command_list.append(" ".join(f"{peer}:2102" for peer in peers))

        config.update(
            {
                "command": " ".join(command_list),
                "ports": ports,
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Zookeeper(Service):
    def __init__(
        self,
        name: str = "zookeeper",
        image: str = "confluentinc/cp-zookeeper",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 2181,
        environment: List[str] = ["ZOOKEEPER_CLIENT_PORT=2181"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "environment": environment,
            },
        )


class Kafka(Service):
    def __init__(
        self,
        name: str = "kafka",
        image: str = "confluentinc/cp-kafka",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 9092,
        auto_create_topics: bool = False,
        broker_id: int = 1,
        offsets_topic_replication_factor: int = 1,
        environment: List[str] = [
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
            "KAFKA_MIN_INSYNC_REPLICAS=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_MESSAGE_MAX_BYTES=15728640",
            "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=100",
        ],
        depends_on: List[str] = ["zookeeper"],
        volumes: List[str] = [],
        listener_type: str = "PLAINTEXT",
    ) -> None:
        environment = [
            *environment,
            f"KAFKA_ADVERTISED_LISTENERS={listener_type}://{name}:{port}",
            f"KAFKA_BROKER_ID={broker_id}",
        ]
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": [port],
            "environment": [
                *environment,
                f"KAFKA_AUTO_CREATE_TOPICS_ENABLE={auto_create_topics}",
                f"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR={offsets_topic_replication_factor}",
            ],
            "depends_on": depends_on,
            "volumes": volumes,
        }
        super().__init__(name=name, config=config)


class Redpanda(Service):
    def __init__(
        self,
        name: str = "redpanda",
        version: str = "v21.11.13",
        image: Optional[str] = None,
        aliases: Optional[List[str]] = None,
        ports: Optional[List[int]] = None,
    ) -> None:
        if image is None:
            image = f"vectorized/redpanda:{version}"

        if ports is None:
            ports = [9092, 8081]

        # The Redpanda container provides both a Kafka and a Schema Registry replacement
        if aliases is None:
            aliases = ["kafka", "schema-registry"]

        # Most of these options are simply required when using Redpanda in Docker.
        # See: https://vectorized.io/docs/quick-start-docker/#Single-command-for-a-1-node-cluster
        # The `enable_transactions` and `enable_idempotence` feature flags enable
        # features Materialize requires that are present by default in Apache Kafka
        # but not in Redpanda.

        command_list = [
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp=1",
            "--memory=1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            '--set "redpanda.enable_transactions=true"',
            '--set "redpanda.enable_idempotence=true"',
            '--set "redpanda.auto_create_topics_enabled=false"',
            f"--advertise-kafka-addr kafka:{ports[0]}",
        ]

        config: ServiceConfig = {
            "image": image,
            "ports": ports,
            "command": " ".join(command_list),
            "networks": {"default": {"aliases": aliases}},
        }

        super().__init__(name=name, config=config)


class SchemaRegistry(Service):
    def __init__(
        self,
        name: str = "schema-registry",
        image: str = "confluentinc/cp-schema-registry",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: int = 8081,
        kafka_servers: List[Tuple[str, str]] = [("kafka", "9092")],
        bootstrap_server_type: str = "PLAINTEXT",
        environment: List[str] = [
            # NOTE(guswynn): under docker, kafka *can* be really slow, which means
            # the default of 500ms won't work, so we give it PLENTY of time
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
        ],
        depends_on: Optional[List[str]] = None,
        volumes: List[str] = [],
    ) -> None:
        bootstrap_servers = ",".join(
            f"{bootstrap_server_type}://{kafka}:{port}" for kafka, port in kafka_servers
        )
        environment = [
            *environment,
            f"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS={bootstrap_servers}",
        ]
        kafka_hosts = [kafka for kafka, _ in kafka_servers]
        super().__init__(
            name=name,
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on or [*kafka_hosts, "zookeeper"],
                "volumes": volumes,
            },
        )


class MySql(Service):
    def __init__(
        self,
        mysql_root_password: str,
        name: str = "mysql",
        image: str = "mysql:8.0.27",
        command: Optional[str] = None,
        port: int = 3306,
        environment: Optional[List[str]] = None,
        volumes: list[str] = ["mydata:/var/lib/mysql-files"],
    ) -> None:
        if environment is None:
            environment = []
        environment.append(f"MYSQL_ROOT_PASSWORD={mysql_root_password}")

        if not command:
            command = "\n".join(
                [
                    "--default-authentication-plugin=mysql_native_password",
                    "--secure-file-priv=/var/lib/mysql-files",
                ]
            )

        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
                "command": command,
                "volumes": volumes,
            },
        )

        self.mysql_root_password = mysql_root_password


class Postgres(Service):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        image: Optional[str] = None,
        port: int = 5432,
        command: str = "postgres -c wal_level=logical -c max_wal_senders=20 -c max_replication_slots=20",
        environment: List[str] = ["POSTGRESDB=postgres", "POSTGRES_PASSWORD=postgres"],
    ) -> None:
        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}
        config.update(
            {
                "command": command,
                "ports": [port],
                "environment": environment,
            }
        )
        super().__init__(name=name, config=config)


class SqlServer(Service):
    def __init__(
        self,
        sa_password: str,  # At least 8 characters including uppercase, lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        name: str = "sql-server",
        image: str = "mcr.microsoft.com/mssql/server",
        environment: List[str] = [
            "ACCEPT_EULA=Y",
            "MSSQL_PID=Developer",
            "MSSQL_AGENT_ENABLED=True",
        ],
    ) -> None:
        environment.append(f"SA_PASSWORD={sa_password}")
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [1433],
                "environment": environment,
            },
        )
        self.sa_password = sa_password


class Debezium(Service):
    def __init__(
        self,
        name: str = "debezium",
        image: str = f"debezium/connect:{DEFAULT_DEBEZIUM_VERSION}",
        port: int = 8083,
        environment: List[str] = [
            "BOOTSTRAP_SERVERS=kafka:9092",
            "CONFIG_STORAGE_TOPIC=connect_configs",
            "OFFSET_STORAGE_TOPIC=connect_offsets",
            "STATUS_STORAGE_TOPIC=connect_statuses",
            # We don't support JSON, so ensure that connect uses AVRO to encode messages and CSR to
            # record the schema
            "KEY_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
            "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        ],
        depends_on: List[str] = ["kafka", "schema-registry"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on,
            },
        )


class Toxiproxy(Service):
    def __init__(
        self,
        name: str = "toxiproxy",
        image: str = "shopify/toxiproxy:2.1.4",
        port: int = 8474,
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
            },
        )


class Squid(Service):
    """
    An HTTP forward proxy, used in some workflows to test whether Materialize can correctly route
    traffic via the proxy.
    """

    def __init__(
        self,
        name: str = "squid",
        image: str = "sameersbn/squid:3.5.27-2",
        port: int = 3128,
        volumes: List[str] = ["./squid.conf:/etc/squid/squid.conf"],
    ) -> None:
        super().__init__(
            name=name,
            config={"image": image, "ports": [port], "volumes": volumes},
        )


class Localstack(Service):
    def __init__(
        self,
        name: str = "localstack",
        image: str = f"localstack/localstack:0.13.1",
        port: int = 4566,
        environment: List[str] = ["HOSTNAME_EXTERNAL=localstack"],
        volumes: List[str] = ["/var/run/docker.sock:/var/run/docker.sock"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
                "volumes": volumes,
            },
        )


class Testdrive(Service):
    def __init__(
        self,
        name: str = "testdrive",
        mzbuild: str = "testdrive",
        materialized_url: str = "postgres://materialize@materialized:6875",
        materialized_params: Dict[str, str] = {},
        kafka_url: str = "kafka:9092",
        kafka_default_partitions: Optional[int] = None,
        no_reset: bool = False,
        default_timeout: str = "120s",
        seed: Optional[int] = None,
        consistent_seed: bool = False,
        validate_data_dir: bool = True,
        validate_postgres_stash: bool = False,
        entrypoint: Optional[List[str]] = None,
        entrypoint_extra: List[str] = [],
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        volumes_extra: Optional[List[str]] = None,
        volume_workdir: str = ".:/workdir",
        propagate_uid_gid: bool = True,
        forward_buildkite_shard: bool = False,
        aws_region: Optional[str] = None,
        aws_endpoint: str = "http://localstack:4566",
    ) -> None:
        if environment is None:
            environment = [
                "TMPDIR=/share/tmp",
                "MZ_SOFT_ASSERTIONS=1",
                # Please think twice before forwarding additional environment
                # variables from the host, as it's easy to write tests that are
                # then accidentally dependent on the state of the host machine.
                #
                # To pass arguments to a testdrive script, use the `--var` CLI
                # option rather than environment variables.
                "MZ_LOG_FILTER",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ]

        if volumes is None:
            volumes = [*DEFAULT_MZ_VOLUMES]
        if volumes_extra:
            volumes.extend(volumes_extra)
        volumes.append(volume_workdir)

        if entrypoint is None:
            entrypoint = [
                "testdrive",
                f"--kafka-addr={kafka_url}",
                "--schema-registry-url=http://schema-registry:8081",
                f"--materialized-url={materialized_url}",
            ]

        if aws_region:
            entrypoint.append(f"--aws-region={aws_region}")

        if aws_endpoint and not aws_region:
            entrypoint.append(f"--aws-endpoint={aws_endpoint}")

        if validate_data_dir:
            entrypoint.append("--validate-data-dir=/mzdata")

        if validate_postgres_stash:
            entrypoint.append(
                "--validate-postgres-stash=postgres://materialize@materialized/materialize?options=--search_path=catalog"
            )

        if no_reset:
            entrypoint.append("--no-reset")

        for (k, v) in materialized_params.items():
            entrypoint.append(f"--materialized-param={k}={v}")

        entrypoint.append(f"--default-timeout={default_timeout}")

        if kafka_default_partitions:
            entrypoint.append(f"--kafka-default-partitions={kafka_default_partitions}")

        if forward_buildkite_shard:
            shard = os.environ.get("BUILDKITE_PARALLEL_JOB")
            shard_count = os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT")
            if shard:
                entrypoint += [f"--shard={shard}"]
            if shard_count:
                entrypoint += [f"--shard-count={shard_count}"]

        if seed is not None and consistent_seed:
            raise RuntimeError("Can't pass `seed` and `consistent_seed` at same time")
        elif consistent_seed:
            entrypoint.append(f"--seed={random.getrandbits(32)}")
        elif seed is not None:
            entrypoint.append(f"--seed={seed}")

        entrypoint.extend(entrypoint_extra)

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "entrypoint": entrypoint,
                "environment": environment,
                "volumes": volumes,
                "propagate_uid_gid": propagate_uid_gid,
                "init": True,
            },
        )


class TestCerts(Service):
    def __init__(
        self,
        name: str = "test-certs",
    ) -> None:
        super().__init__(
            name="test-certs",
            config={
                "mzbuild": "test-certs",
                "volumes": ["secrets:/secrets"],
            },
        )


class SqlLogicTest(Service):
    def __init__(
        self,
        name: str = "sqllogictest-svc",
        mzbuild: str = "sqllogictest",
        environment: List[str] = [
            "PGUSER=postgres",
            "PGHOST=postgres",
            "PGPASSWORD=postgres",
            "MZ_SOFT_ASSERTIONS=1",
        ],
        volumes: List[str] = ["../..:/workdir"],
        depends_on: List[str] = ["postgres"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "environment": environment,
                "volumes": volumes,
                "depends_on": depends_on,
                "propagate_uid_gid": True,
                "init": True,
            },
        )


class Kgen(Service):
    def __init__(
        self,
        name: str = "kgen",
        mzbuild: str = "kgen",
        depends_on: List[str] = ["kafka"],
    ) -> None:
        entrypoint = [
            "kgen",
            "--bootstrap-server=kafka:9092",
        ]

        if "schema-registry" in depends_on:
            entrypoint.append("--schema-registry-url=http://schema-registry:8081")

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "depends_on": depends_on,
                "entrypoint": entrypoint,
            },
        )


class Metabase(Service):
    def __init__(self, name: str = "metabase") -> None:
        super().__init__(
            name=name,
            config={
                "image": "metabase/metabase:v0.41.4",
                "ports": ["3000"],
            },
        )
