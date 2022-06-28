# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
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
    Zookeeper(
        environment=[
            "ZOOKEEPER_CLIENT_PORT=2181",
            # Despite the environment variable name, these are JVM options that are
            # passed through to Zookeeper.
            "KAFKA_OPTS=-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider -Dzookeeper.sessionRequireClientSASLAuth=true -Djava.security.auth.login.config=/etc/zookeeper/sasl.jaas.config",
        ],
        volumes=["./sasl.jaas.config:/etc/zookeeper/sasl.jaas.config"],
    ),
    Kafka(
        environment=[
            #            "KAFKA_INTER_BROKER_LISTENER_NAME=SSL",
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN",
            "KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN",
            "KAFKA_SSL_KEYSTORE_FILENAME=kafka.keystore.jks",
            "KAFKA_SSL_KEYSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_KEY_CREDENTIALS=cert_creds",
            "KAFKA_SSL_TRUSTSTORE_FILENAME=kafka.truststore.jks",
            "KAFKA_SSL_TRUSTSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_CLIENT_AUTH=required",
            "KAFKA_SECURITY_INTER_BROKER_PROTOCOL=SASL_SSL",
            "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/sasl.jaas.config",
        ],
        listener_type="SASL_SSL",
        volumes=[
            "secrets:/etc/kafka/secrets",
            "./sasl.jaas.config:/etc/kafka/sasl.jaas.config",
        ],
    ),
    SchemaRegistry(
        environment=[
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
            "SCHEMA_REGISTRY_LISTENERS=https://0.0.0.0:8081",
            "SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM=PLAIN",
            "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_SSL",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL=https",
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES=user",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry",
            "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/sasl.jaas.config",
            "SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/sasl.jaas.config",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
            "./sasl.jaas.config:/etc/schema-registry/sasl.jaas.config",
            "./users.properties:/etc/schema-registry/users.properties",
        ],
        bootstrap_server_type="SASL_SSL",
    ),
    Materialized(
        environment_extra=[
            "SASL_PASSWORD=sekurity",
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
            "--kafka-option=security.protocol=SASL_SSL "
            "--kafka-option=sasl.mechanism=PLAIN "
            "--kafka-option=sasl.username=materialize "
            "--kafka-option=sasl.password=sekurity "
            "--schema-registry-url=https://materialize:sekurity@schema-registry:8081 "
            "--materialize-url=postgres://materialize@materialized:6875 "
            "--cert=/share/secrets/producer.p12 "
            "--cert-password=mzmzmz "
            '--var=ca="$$(</share/secrets/ca.crt)" '
            '"$$@"',
        ],
        volumes_extra=["secrets:/share/secrets"],
        propagate_uid_gid=False,
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("test-certs")
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
    c.up("materialized")
    c.wait_for_materialized()
    c.run("testdrive", "*.td")
