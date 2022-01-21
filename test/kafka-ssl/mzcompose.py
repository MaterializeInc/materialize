# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    TestCerts,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    TestCerts(),
    Zookeeper(),
    Kafka(
        depends_on=["zookeeper", "test-certs"],
        environment=[
            # Default
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
            "KAFKA_MIN_INSYNC_REPLICAS=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_MESSAGE_MAX_BYTES=15728640",
            "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
            # For this test
            "KAFKA_SSL_KEYSTORE_FILENAME=kafka.keystore.jks",
            "KAFKA_SSL_KEYSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_KEY_CREDENTIALS=cert_creds",
            "KAFKA_SSL_TRUSTSTORE_FILENAME=kafka.truststore.jks",
            "KAFKA_SSL_TRUSTSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_CLIENT_AUTH=required",
            "KAFKA_SECURITY_INTER_BROKER_PROTOCOL=SSL",
        ],
        listener_type="SSL",
        volumes=["secrets:/etc/kafka/secrets"],
    ),
    SchemaRegistry(
        depends_on=["kafka", "zookeeper", "test-certs"],
        environment=[
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=schema-registry",
            "SCHEMA_REGISTRY_LISTENERS=https://0.0.0.0:8081",
            "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181",
            "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SSL",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL=https",
            "SCHEMA_REGISTRY_SSL_CLIENT_AUTH=true",
        ],
        volumes=["secrets:/etc/schema-registry/secrets"],
        bootstrap_server_type="SSL",
    ),
    Materialized(
        environment=[
            "SSL_KEY_PASSWORD=mzmzmz",
        ],
        volumes_extra=["secrets:/share/secrets"],
    ),
    Testdrive(
        entrypoint=[
            "bash",
            "-c",
            "cp /share/secrets/ca.crt /usr/local/share/ca-certificates/ca.crt && "
            "update-ca-certificates && "
            "testdrive "
            "--kafka-addr=kafka:9092 "
            "--schema-registry-url=https://schema-registry:8081 "
            "--materialized-url=postgres://materialize@materialized:6875 "
            "--cert=/share/secrets/producer.p12 "
            "--cert-password=mzmzmz "
            '"$$@"',
        ],
        volumes_extra=["secrets:/share/secrets"],
        # Required to install root certs above
        propagate_uid_gid=False,
    ),
]


def workflow_testdrive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """ "Run testdrive against an SSL-enabled Confluent Platform."""
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()
    c.start_and_wait_for_tcp(
        services=[
            "zookeeper",
            "kafka",
            "schema-registry",
            "materialized",
        ]
    )
    c.run("testdrive-svc", *args.files)
