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
import tempfile
from typing import Dict, List, Optional, Tuple, Union

import toml

from materialize import ROOT
from materialize.mzcompose import (
    Service,
    ServiceConfig,
    ServiceDependency,
    ServiceHealthcheck,
    loader,
)
from materialize.util import MzVersion

DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.0.5"

# Be sure to use a `X.Y.Z.Final` tag here; `X.Y` tags refer to the latest
# minor version in the release series, and minor versions have been known to
# introduce breakage.
DEFAULT_DEBEZIUM_VERSION = "1.9.6.Final"

LINT_DEBEZIUM_VERSIONS = ["1.4", "1.5", "1.6"]

DEFAULT_MZ_VOLUMES = [
    "mzdata:/mzdata",
    "mydata:/var/lib/mysql-files",
    "tmp:/share/tmp",
]

# TODO(benesch): change to `docker-mzcompose` once v0.39 ships.
DEFAULT_MZ_ENVIRONMENT_ID = "mzcompose-test-00000000-0000-0000-0000-000000000000-0"


class Materialized(Service):
    class Size:
        DEFAULT_SIZE = 4

    def __init__(
        self,
        name: str = "materialized",
        image: Optional[str] = None,
        environment_extra: List[str] = [],
        volumes_extra: List[str] = [],
        depends_on: List[str] = [],
        memory: Optional[str] = None,
        options: List[str] = [],
        persist_blob_url: Optional[str] = None,
        default_size: int = Size.DEFAULT_SIZE,
        environment_id: Optional[str] = None,
        propagate_crashes: bool = True,
        external_cockroach: bool = False,
        external_minio: bool = False,
        unsafe_mode: bool = True,
        restart: Optional[str] = None,
        use_default_volumes: bool = True,
        ports: Optional[List[str]] = None,
        system_parameter_defaults: Optional[List[str]] = None,
        additional_system_parameter_defaults: Optional[List[str]] = None,
    ) -> None:
        depends_on: Dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }

        environment = [
            "MZ_SOFT_ASSERTIONS=1",
            # TODO(benesch): remove the following environment variables
            # after v0.38 ships, since these environment variables will be
            # baked into the Docker image.
            "MZ_ORCHESTRATOR=process",
            # The following settings can not be baked in the default image, as they
            # are enabled for testing purposes only
            "ORCHESTRATOR_PROCESS_TCP_PROXY_LISTEN_ADDR=0.0.0.0",
            "ORCHESTRATOR_PROCESS_PROMETHEUS_SERVICE_DISCOVERY_DIRECTORY=/mzdata/prometheus",
            # Please think twice before forwarding additional environment
            # variables from the host, as it's easy to write tests that are
            # then accidentally dependent on the state of the host machine.
            #
            # To dynamically change the environment during a workflow run,
            # use Composition.override.
            "MZ_LOG_FILTER",
            "CLUSTERD_LOG_FILTER",
            "BOOTSTRAP_ROLE=materialize",
            *environment_extra,
        ]

        if system_parameter_defaults is None:
            system_parameter_defaults = [
                "persist_sink_minimum_batch_updates=128",
                "enable_multi_worker_storage_persist_sink=true",
                "storage_persist_sink_minimum_batch_updates=100",
            ]

        if additional_system_parameter_defaults is not None:
            system_parameter_defaults += additional_system_parameter_defaults

        if len(system_parameter_defaults) > 0:
            environment += [
                "MZ_SYSTEM_PARAMETER_DEFAULT=" + ";".join(system_parameter_defaults)
            ]

        command = []

        if unsafe_mode:
            command += ["--unsafe-mode"]

        if not environment_id:
            environment_id = DEFAULT_MZ_ENVIRONMENT_ID
        command += [f"--environment-id={environment_id}"]

        if external_minio:
            depends_on["minio"] = {"condition": "service_healthy"}
            persist_blob_url = "s3://minioadmin:minioadmin@persist/persist?endpoint=http://minio:9000/&region=minio"

        if persist_blob_url:
            command.append(f"--persist-blob-url={persist_blob_url}")

        if propagate_crashes:
            command += ["--orchestrator-process-propagate-crashes"]

        self.default_storage_size = (
            default_size
            if image
            and "latest" not in image
            and "devel" not in image
            and "unstable" not in image
            and MzVersion.parse_mz(image.split(":")[1]) < MzVersion.parse("0.41.0")
            else "1"
            if default_size == 1
            else f"{default_size}-1"
        )

        self.default_replica_size = (
            "1" if default_size == 1 else f"{default_size}-{default_size}"
        )
        command += [
            # Issue #15858 prevents the habitual use of large introspection
            # clusters, so we leave the builtin cluster replica size as is.
            # f"--bootstrap-builtin-cluster-replica-size={self.default_replica_size}",
            f"--bootstrap-default-cluster-replica-size={self.default_replica_size}",
            f"--default-storage-host-size={self.default_storage_size}",
        ]

        if external_cockroach:
            depends_on["cockroach"] = {"condition": "service_healthy"}
            command += [
                "--adapter-stash-url=postgres://root@cockroach:26257?options=--search_path=adapter",
                "--storage-stash-url=postgres://root@cockroach:26257?options=--search_path=storage",
                "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
            ]

        command += [
            "--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0",
            "--orchestrator-process-prometheus-service-discovery-directory=/mzdata/prometheus",
        ]

        command += options

        config: ServiceConfig = {}

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "materialized"

        if restart:
            config["restart"] = restart

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        volumes = []
        if use_default_volumes:
            volumes += DEFAULT_MZ_VOLUMES
        volumes += volumes_extra

        config.update(
            {
                "depends_on": depends_on,
                "command": command,
                "ports": [6875, 6876, 6877, 6878, 26257],
                "environment": environment,
                "volumes": volumes,
                "tmpfs": ["/tmp"],
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:6878/api/readyz"],
                    "interval": "1s",
                    # A fully loaded Materialize can take a long time to start.
                    "start_period": "600s",
                },
            }
        )

        if ports:
            config.update(
                {
                    "allow_host_ports": True,
                    "ports": ports,
                }
            )

        super().__init__(name=name, config=config)


class Clusterd(Service):
    def __init__(
        self,
        name: str = "clusterd",
        image: Optional[str] = None,
        environment_extra: List[str] = [],
        memory: Optional[str] = None,
        options: List[str] = [],
    ) -> None:
        environment = [
            "CLUSTERD_LOG_FILTER",
            "MZ_SOFT_ASSERTIONS=1",
            *environment_extra,
        ]

        command = []

        command += options

        config: ServiceConfig = {}

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "clusterd"

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "command": command,
                "ports": [2100, 2101],
                "environment": environment,
                "volumes": DEFAULT_MZ_VOLUMES,
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
        volumes: List[str] = [],
        environment: List[str] = ["ZOOKEEPER_CLIENT_PORT=2181"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": f"{image}:{tag}",
                "ports": [port],
                "volumes": volumes,
                "environment": environment,
                "healthcheck": {
                    "test": ["CMD", "nc", "-z", "localhost", "2181"],
                    "interval": "1s",
                    "start_period": "120s",
                },
            },
        )


class Kafka(Service):
    def __init__(
        self,
        name: str = "kafka",
        image: str = "confluentinc/cp-kafka",
        tag: str = DEFAULT_CONFLUENT_PLATFORM_VERSION,
        port: Union[str, int] = 9092,
        allow_host_ports: bool = False,
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
        extra_environment: List[str] = [],
        depends_on_extra: List[str] = [],
        volumes: List[str] = [],
        listener_type: str = "PLAINTEXT",
    ) -> None:
        environment = [
            *environment,
            f"KAFKA_ADVERTISED_LISTENERS={listener_type}://{name}:{port}",
            f"KAFKA_BROKER_ID={broker_id}",
            *extra_environment,
        ]
        config: ServiceConfig = {
            "image": f"{image}:{tag}",
            "ports": [port],
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
        super().__init__(name=name, config=config)


class Redpanda(Service):
    def __init__(
        self,
        name: str = "redpanda",
        version: str = "v22.3.13",
        auto_create_topics: bool = False,
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
            "--set",
            "redpanda.enable_transactions=true",
            "--set",
            "redpanda.enable_idempotence=true",
            "--set",
            f"redpanda.auto_create_topics_enabled={auto_create_topics}",
            "--set",
            f"--advertise-kafka-addr=kafka:{ports[0]}",
        ]

        config: ServiceConfig = {
            "image": image,
            "ports": ports,
            "command": command_list,
            "networks": {"default": {"aliases": aliases}},
            "healthcheck": {
                "test": ["CMD", "curl", "-f", "localhost:9644/v1/status/ready"],
                "interval": "1s",
                "start_period": "120s",
            },
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
        depends_on_extra: List[str] = [],
        volumes: List[str] = [],
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


class MySql(Service):
    DEFAULT_ROOT_PASSWORD = "p@ssw0rd"

    def __init__(
        self,
        root_password: str = DEFAULT_ROOT_PASSWORD,
        name: str = "mysql",
        image: str = "mysql:8.0.32",
        port: int = 3306,
        volumes: list[str] = ["mydata:/var/lib/mysql-files"],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "init": True,
                "ports": [port],
                "environment": [
                    f"MYSQL_ROOT_PASSWORD={root_password}",
                ],
                "command": [
                    "--default-authentication-plugin=mysql_native_password",
                    "--secure-file-priv=/var/lib/mysql-files",
                ],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "mysqladmin",
                        "ping",
                        f"--password={root_password}",
                    ],
                    "interval": "1s",
                    "start_period": "60s",
                },
                "volumes": volumes,
            },
        )


class Cockroach(Service):
    DEFAULT_COCKROACH_TAG = "v23.1.1"

    def __init__(
        self,
        name: str = "cockroach",
        aliases: List[str] = ["cockroach"],
        image: Optional[str] = None,
        command: Optional[List[str]] = None,
        setup_materialize: bool = True,
        in_memory: bool = False,
        healthcheck: Optional[ServiceHealthcheck] = None,
        restart: str = "no",
    ):
        volumes = []

        if image is None:
            image = f"cockroachdb/cockroach:{Cockroach.DEFAULT_COCKROACH_TAG}"

        if command is None:
            command = ["start-single-node", "--insecure"]

        if setup_materialize:
            path = os.path.relpath(
                ROOT / "misc" / "cockroach" / "setup_materialize.sql",
                loader.composition_path,
            )
            volumes += [f"{path}:/docker-entrypoint-initdb.d/setup_materialize.sql"]

        if in_memory:
            command.append("--store=type=mem,size=2G")

        if healthcheck is None:
            healthcheck = {
                # init_success is a file created by the Cockroach container entrypoint
                "test": "[ -f init_success ] && curl --fail 'http://localhost:8080/health?ready=1'",
                "timeout": "5s",
                "interval": "1s",
                "start_period": "30s",
            }

        super().__init__(
            name=name,
            config={
                "image": image,
                "networks": {"default": {"aliases": aliases}},
                "ports": [26257],
                "command": command,
                "volumes": volumes,
                "init": True,
                "healthcheck": healthcheck,
                "restart": restart,
            },
        )


class Postgres(Service):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        image: Optional[str] = None,
        port: int = 5432,
        command: List[str] = [
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=20",
            "-c",
            "max_replication_slots=20",
            "-c",
            "max_connections=5000",
        ],
        environment: List[str] = ["POSTGRESDB=postgres", "POSTGRES_PASSWORD=postgres"],
    ) -> None:
        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}
        config.update(
            {
                "command": command,
                "ports": [port],
                "environment": environment,
                "healthcheck": {
                    "test": ["CMD", "pg_isready"],
                    "interval": "1s",
                    "start_period": "30s",
                },
            }
        )
        super().__init__(name=name, config=config)


class SqlServer(Service):
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        image: str = "mcr.microsoft.com/mssql/server",
        environment_extra: List[str] = [],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [1433],
                "environment": [
                    "ACCEPT_EULA=Y",
                    "MSSQL_PID=Developer",
                    "MSSQL_AGENT_ENABLED=True",
                    f"SA_PASSWORD={sa_password}",
                    *environment_extra,
                ],
            },
        )
        self.sa_password = sa_password


class Debezium(Service):
    def __init__(
        self,
        name: str = "debezium",
        image: str = f"debezium/connect:{DEFAULT_DEBEZIUM_VERSION}",
        port: int = 8083,
        redpanda: bool = False,
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
            "CONNECT_OFFSET_COMMIT_POLICY=AlwaysCommitOffsetPolicy",
            "CONNECT_ERRORS_RETRY_TIMEOUT=60000",
            "CONNECT_ERRORS_RETRY_DELAY_MAX_MS=1000",
        ],
    ) -> None:
        depends_on: Dict[str, ServiceDependency] = {
            "kafka": {"condition": "service_healthy"},
            "schema-registry": {"condition": "service_healthy"},
        }
        if redpanda:
            depends_on = {"redpanda": {"condition": "service_healthy"}}
        super().__init__(
            name=name,
            config={
                "image": image,
                "init": True,
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on,
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:8083"],
                    "interval": "1s",
                    "start_period": "120s",
                },
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
                "healthcheck": {
                    "test": ["CMD", "nc", "-z", "localhost", "8474"],
                    "interval": "1s",
                    "start_period": "30s",
                },
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
        image: str = "localstack/localstack:0.13.1",
        port: int = 4566,
        environment: List[str] = ["HOSTNAME_EXTERNAL=localstack"],
        volumes: List[str] = ["/var/run/docker.sock:/var/run/docker.sock"],
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
                    "test": ["CMD", "curl", "-f", "localhost:4566/health"],
                    "interval": "1s",
                    "start_period": "120s",
                },
            },
        )


class Minio(Service):
    def __init__(
        self,
        name: str = "minio",
        image: str = "minio/minio:RELEASE.2022-09-25T15-44-53Z.fips",
        setup_materialize: bool = False,
    ) -> None:
        # We can pre-create buckets in minio by creating subdirectories in
        # /data. A bit gross to do this via a shell command, but it's net
        # less complicated than using a separate setup container that runs `mc`.
        command = "minio server /data --console-address :9001"
        if setup_materialize:
            command = f"mkdir -p /data/persist && {command}"
        super().__init__(
            name=name,
            config={
                "entrypoint": ["sh", "-c"],
                "command": [command],
                "image": image,
                "ports": [9000, 9001],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "curl",
                        "--fail",
                        "http://localhost:9000/minio/health/live",
                    ],
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )


class Testdrive(Service):
    def __init__(
        self,
        name: str = "testdrive",
        mzbuild: str = "testdrive",
        materialize_url: str = "postgres://materialize@materialized:6875",
        materialize_url_internal: str = "postgres://materialize@materialized:6877",
        materialize_params: Dict[str, str] = {},
        kafka_url: str = "kafka:9092",
        kafka_default_partitions: Optional[int] = None,
        kafka_args: Optional[str] = None,
        schema_registry_url: str = "http://schema-registry:8081",
        no_reset: bool = False,
        default_timeout: str = "120s",
        seed: Optional[int] = None,
        consistent_seed: bool = False,
        validate_postgres_stash: Optional[str] = None,
        entrypoint: Optional[List[str]] = None,
        entrypoint_extra: List[str] = [],
        environment: Optional[List[str]] = None,
        volumes_extra: List[str] = [],
        volume_workdir: str = ".:/workdir",
        propagate_uid_gid: bool = True,
        forward_buildkite_shard: bool = False,
        aws_region: Optional[str] = None,
        aws_endpoint: Optional[str] = "http://localstack:4566",
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

        volumes = [
            volume_workdir,
            *(v for v in DEFAULT_MZ_VOLUMES if v.startswith("tmp:")),
        ]
        if volumes_extra:
            volumes.extend(volumes_extra)

        if entrypoint is None:
            entrypoint = [
                "testdrive",
                f"--kafka-addr={kafka_url}",
                f"--schema-registry-url={schema_registry_url}",
                f"--materialize-url={materialize_url}",
                f"--materialize-internal-url={materialize_url_internal}",
            ]

        if aws_region:
            entrypoint.append(f"--aws-region={aws_region}")

        if aws_endpoint and not aws_region:
            entrypoint.append(f"--aws-endpoint={aws_endpoint}")

        if validate_postgres_stash:
            entrypoint.append(
                f"--validate-postgres-stash=postgres://root@{validate_postgres_stash}:26257?options=--search_path=adapter"
            )

        if no_reset:
            entrypoint.append("--no-reset")

        for (k, v) in materialize_params.items():
            entrypoint.append(f"--materialize-param={k}={v}")

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
                # Container must stay alive indefinitely to be considered
                # healthy by `docker compose up --wait`.
                "command": ["sleep", "infinity"],
                "init": True,
                "mzbuild": "test-certs",
                "volumes": ["secrets:/secrets"],
            },
        )


class SqlLogicTest(Service):
    def __init__(
        self,
        name: str = "sqllogictest",
        mzbuild: str = "sqllogictest",
        environment: List[str] = [
            "MZ_SOFT_ASSERTIONS=1",
        ],
        volumes: List[str] = ["../..:/workdir"],
        depends_on: List[str] = ["cockroach"],
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
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:3000/api/health"],
                    "interval": "1s",
                    "start_period": "300s",
                },
            },
        )


class SshBastionHost(Service):
    def __init__(
        self,
        name: str = "ssh-bastion-host",
        max_startups: Optional[str] = None,
    ) -> None:
        setup_path = os.path.relpath(
            ROOT / "misc" / "images" / "sshd" / "setup.sh",
            loader.composition_path,
        )
        super().__init__(
            name=name,
            config={
                "image": "panubo/sshd:1.5.0",
                "init": True,
                "ports": ["22"],
                "environment": [
                    "SSH_USERS=mz:1000:1000",
                    "TCP_FORWARDING=true",
                    *([f"MAX_STARTUPS={max_startups}"] if max_startups else []),
                ],
                "volumes": [f"{setup_path}:/etc/entrypoint.d/setup.sh"],
                "healthcheck": {
                    "test": "[ -f /var/run/sshd/sshd.pid ]",
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "60s",
                },
            },
        )


class Mz(Service):
    def __init__(
        self,
        *,
        name: str = "mz",
        region: str = "aws/us-east-1",
        environment: str = "staging",
        username: str,
        app_password: str,
    ) -> None:
        # We must create the temporary config file in a location
        # that is accessible on the same path in both the ci-builder
        # container and the host that runs the docker daemon
        # $TMP does not guarantee that, but loader.composition_path does.
        config = tempfile.NamedTemporaryFile(
            dir=loader.composition_path,
            prefix="tmp_",
            suffix=".toml",
            mode="w",
            delete=False,
        )
        toml.dump(
            {
                "current_profile": "default",
                "profiles": {
                    "default": {
                        "email": username,
                        "app-password": app_password,
                        "region": region,
                        "endpoint": f"https://{environment}.cloud.materialize.com/",
                    },
                },
            },
            config,
        )
        config.close()
        super().__init__(
            name=name,
            config={
                "mzbuild": "mz",
                "volumes": [f"{config.name}:/root/.config/mz/profiles.toml"],
            },
        )


class Prometheus(Service):
    def __init__(self, name: str = "prometheus") -> None:
        super().__init__(
            name=name,
            config={
                "image": "prom/prometheus:v2.41.0",
                "ports": ["9090"],
                "volumes": [
                    str(ROOT / "misc" / "mzcompose" / "prometheus" / "prometheus.yml")
                    + ":/etc/prometheus/prometheus.yml",
                    "mzdata:/mnt/mzdata",
                ],
            },
        )


class Grafana(Service):
    def __init__(self, name: str = "grafana") -> None:
        super().__init__(
            name=name,
            config={
                "image": "grafana/grafana:9.3.2",
                "ports": ["3000"],
                "environment": [
                    "GF_AUTH_ANONYMOUS_ENABLED=true",
                    "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
                ],
                "volumes": [
                    str(ROOT / "misc" / "mzcompose" / "grafana" / "datasources")
                    + ":/etc/grafana/provisioning/datasources",
                ],
            },
        )
