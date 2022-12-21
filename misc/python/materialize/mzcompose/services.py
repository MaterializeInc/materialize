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

from packaging import version

from materialize.mzcompose import Service, ServiceConfig

DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.0.5"

# Be sure to use a `X.Y.Z.Final` tag here; `X.Y` tags refer to the latest
# minor version in the release series, and minor versions have been known to
# introduce breakage.
DEFAULT_DEBEZIUM_VERSION = "1.9.6.Final"

LINT_DEBEZIUM_VERSIONS = ["1.4", "1.5", "1.6"]

DEFAULT_MZ_VOLUMES = [
    "mzdata:/mzdata",
    "pgdata:/cockroach-data",
    "mydata:/var/lib/mysql-files",
    "tmp:/share/tmp",
]

DEFAULT_MZ_ENVIRONMENT_ID = "mzcompose-test-00000000-0000-0000-0000-000000000000-0"


class Materialized(Service):
    class Size:
        DEFAULT_SIZE = 4

    def __init__(
        self,
        name: str = "materialized",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        ports: Optional[List[str]] = None,
        extra_ports: List[int] = [],
        memory: Optional[str] = None,
        persist_blob_url: Optional[str] = None,
        data_directory: str = "/mzdata",
        workers: Optional[int] = Size.DEFAULT_SIZE,
        default_size: int = Size.DEFAULT_SIZE,
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        environment_extra: Optional[List[str]] = None,
        forward_aws_credentials: bool = True,
        volumes: Optional[List[str]] = None,
        volumes_extra: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
        allow_host_ports: bool = False,
        environment_id: Optional[str] = None,
        propagate_crashes: bool = True,
    ) -> None:
        if persist_blob_url is None:
            persist_blob_url = f"file://{data_directory}/persist/blob"

        if environment is None:
            environment = [
                "MZ_SOFT_ASSERTIONS=1",
                "MZ_UNSAFE_MODE=1",
                "MZ_EXPERIMENTAL=1",
                f"MZ_ENVIRONMENT_ID={DEFAULT_MZ_ENVIRONMENT_ID}",
                f"MZ_PERSIST_BLOB_URL={persist_blob_url}",
                f"MZ_ORCHESTRATOR=process",
                f"MZ_ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY={data_directory}/secrets",
                # TODO(benesch): remove this legacy environment variable once
                # the upgrade tests no longer test v0.33.
                f"MZ_ORCHESTRATOR_PROCESSDATA_DIRECTORY={data_directory}",
                # Please think twice before forwarding additional environment
                # variables from the host, as it's easy to write tests that are
                # then accidentally dependent on the state of the host machine.
                #
                # To dynamically change the environment during a workflow run,
                # use Composition.override.
                "MZ_LOG_FILTER",
                "STORAGED_LOG_FILTER",
                "COMPUTED_LOG_FILTER",
                "INTERNAL_SQL_LISTEN_ADDR=0.0.0.0:6877",
                "INTERNAL_HTTP_LISTEN_ADDR=0.0.0.0:6878",
            ]

        if forward_aws_credentials:
            environment += [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ]

        if environment_id:
            environment += [f"MZ_ENVIRONMENT_ID={environment_id}"]

        if propagate_crashes:
            environment += ["MZ_ORCHESTRATOR_PROCESS_PROPAGATE_CRASHES=1"]

        self.default_storage_size = default_size
        self.default_replica_size = (
            "1" if default_size == 1 else f"{default_size}-{default_size}"
        )

        environment += [
            # Issue #15858 prevents the habitual use of large introspection clusters,
            # so we are leaving MZ_BOOTSTRAP_BUILTIN_CLUSTER_REPLICA_SIZE as is.
            # f"MZ_BOOTSTRAP_BUILTIN_CLUSTER_REPLICA_SIZE={self.default_replica_size}",
            f"MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE={self.default_replica_size}",
            f"MZ_DEFAULT_STORAGE_HOST_SIZE={self.default_storage_size}",
        ]

        if workers:
            environment += [
                f"MZ_WORKERS={workers}",
            ]

        if environment_extra:
            environment.extend(environment_extra)

        if volumes is None:
            volumes = [*DEFAULT_MZ_VOLUMES]
        if volumes_extra:
            volumes.extend(volumes_extra)

        config_ports: List[Union[str, int]] = (
            [*ports, *extra_ports]
            if ports
            else [6875, 26257, *extra_ports, 6876, 6877, 6878]
        )

        if isinstance(image, str) and ":v" in image:
            requested_version = image.split(":v")[1]
            if version.parse(requested_version) < version.parse("0.27.0"):
                # HTTP and SQL ports in older versions of Materialize are the same
                config_ports.pop()

        command_list = []

        if options:
            if isinstance(options, str):
                command_list.append(options)
            else:
                command_list.extend(options)

        config: ServiceConfig = (
            {"image": image} if image else {"mzbuild": "materialized"}
        )

        config["hostname"] = hostname or name

        # Depending on the docker-compose version, this may either work or be ignored with a warning
        # Unfortunately no portable way of setting the memory limit is known
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        config.update(
            {
                "depends_on": depends_on or [],
                "command": " ".join(command_list),
                "ports": config_ports,
                "environment": environment,
                "volumes": volumes,
                "allow_host_ports": allow_host_ports,
                "tmpfs": ["/tmp"],
            }
        )

        super().__init__(name=name, config=config)


class Computed(Service):
    def __init__(
        self,
        name: str = "computed",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        ports: List[int] = [2100, 2102],
        memory: Optional[str] = None,
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        secrets_reader: str = "process",
        secrets_reader_process_dir: str = "mzdata/secrets",
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

        command_list.append(f"--secrets-reader {secrets_reader}")
        command_list.append(
            f"--secrets-reader-process-dir {secrets_reader_process_dir}"
        )

        config.update(
            {
                "command": " ".join(command_list),
                "ports": ports,
                "environment": environment,
                "volumes": volumes,
            }
        )

        super().__init__(name=name, config=config)


class Storaged(Service):
    def __init__(
        self,
        name: str = "storaged",
        hostname: Optional[str] = None,
        image: Optional[str] = None,
        ports: List[int] = [2100],
        memory: Optional[str] = None,
        options: Optional[Union[str, List[str]]] = "",
        environment: Optional[List[str]] = None,
        volumes: Optional[List[str]] = None,
        workers: Optional[int] = Materialized.Size.DEFAULT_SIZE,
        secrets_reader: str = "process",
        secrets_reader_process_dir: str = "mzdata/secrets",
    ) -> None:
        if environment is None:
            environment = [
                "STORAGED_LOG_FILTER",
                "MZ_SOFT_ASSERTIONS=1",
            ]

        if volumes is None:
            # We currently give computed access to /tmp so that it can load CSV files
            # but this requirement is expected to go away in the future.
            volumes = DEFAULT_MZ_VOLUMES

        config: ServiceConfig = {"image": image} if image else {"mzbuild": "storaged"}

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

        command_list.append(f"--secrets-reader {secrets_reader}")
        command_list.append(
            f"--secrets-reader-process-dir {secrets_reader_process_dir}"
        )

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
        depends_on: List[str] = ["zookeeper"],
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
            "depends_on": depends_on,
            "volumes": volumes,
        }
        super().__init__(name=name, config=config)


class Redpanda(Service):
    def __init__(
        self,
        name: str = "redpanda",
        version: str = "v22.3.8",
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
            '--set "redpanda.enable_transactions=true"',
            '--set "redpanda.enable_idempotence=true"',
            f'--set "redpanda.auto_create_topics_enabled={auto_create_topics}"',
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


class Cockroach(Service):
    def __init__(
        self,
        name: str = "cockroach",
    ):
        super().__init__(
            name="cockroach",
            config={
                "image": "cockroachdb/cockroach:v22.2.0",
                "ports": [26257],
                "command": "start-single-node --insecure",
                "volumes": ["/cockroach/cockroach-data"],
            },
        )


class Postgres(Service):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        image: Optional[str] = None,
        port: int = 5432,
        command: str = "postgres -c wal_level=logical -c max_wal_senders=20 -c max_replication_slots=20 -c max_connections=5000",
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
            "CONNECT_OFFSET_COMMIT_POLICY=AlwaysCommitOffsetPolicy",
            "CONNECT_ERRORS_RETRY_TIMEOUT=60000",
            "CONNECT_ERRORS_RETRY_DELAY_MAX_MS=1000",
        ],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "environment": environment,
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


class Minio(Service):
    def __init__(
        self,
        name: str = "minio",
        image: str = f"minio/minio:RELEASE.2022-09-25T15-44-53Z.fips",
    ) -> None:
        super().__init__(
            name=name,
            config={
                "command": "minio server /data --console-address :9001",
                "image": image,
                "ports": [9000, 9001],
            },
        )


class MinioMc(Service):
    def __init__(
        self,
        name: str = "minio_mc",
        image: str = f"minio/mc:RELEASE.2022-09-16T09-16-47Z",
    ) -> None:
        super().__init__(
            name=name,
            config={"image": image},
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
        no_reset: bool = False,
        default_timeout: str = "120s",
        seed: Optional[int] = None,
        consistent_seed: bool = False,
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
                f"--materialize-url={materialize_url}",
                f"--materialize-internal-url={materialize_url_internal}",
            ]

        if aws_region:
            entrypoint.append(f"--aws-region={aws_region}")

        if aws_endpoint and not aws_region:
            entrypoint.append(f"--aws-endpoint={aws_endpoint}")

        if validate_postgres_stash:
            entrypoint.append(
                "--validate-postgres-stash=postgres://root@materialized:26257?options=--search_path=adapter"
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
            },
        )


class SshBastionHost(Service):
    def __init__(self, name: str = "ssh-bastion-host") -> None:
        super().__init__(
            name=name,
            config={
                "image": "panubo/sshd:1.5.0",
                "ports": ["22"],
                "environment": [
                    "SSH_USERS=mz:1000:1000",
                    "TCP_FORWARDING=true",
                ],
            },
        )
