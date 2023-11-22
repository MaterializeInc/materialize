# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    TestCerts(),
    SshBastionHost(),
    Zookeeper(),
    Kafka(
        depends_on_extra=["test-certs"],
        advertised_listeners=[
            # Using lowercase listener names here bypasses some too-helpful
            # checks in the Docker entrypoint that (incorrectly) attempt to
            # assess the validity of the authentication configuration.
            "plaintext://kafka:9092",
            "ssl://kafka:9093",
            "mssl://kafka:9094",
            "sasl_plaintext://kafka:9095",
            "sasl_ssl://kafka:9096",
            "sasl_mssl://kafka:9097",
        ],
        environment_extra=[
            "ZOOKEEPER_SASL_ENABLED=FALSE",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,ssl:SSL,mssl:SSL,sasl_plaintext:SASL_PLAINTEXT,sasl_ssl:SASL_SSL,sasl_mssl:SASL_SSL",
            "KAFKA_INTER_BROKER_LISTENER_NAME=plaintext",
            "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512",
            "KAFKA_SSL_KEY_PASSWORD=mzmzmz",
            "KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/kafka.keystore.jks",
            "KAFKA_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/kafka.truststore.jks",
            "KAFKA_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/jaas.config",
            "KAFKA_LISTENER_NAME_MSSL_SSL_CLIENT_AUTH=required",
            "KAFKA_LISTENER_NAME_SASL__MSSL_SSL_CLIENT_AUTH=required",
            "KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer",
            "KAFKA_SUPER_USERS=User:materialize;User:CN=materialized;User:ANONYMOUS",
        ],
        volumes=[
            "secrets:/etc/kafka/secrets",
            "./kafka.jaas.config:/etc/kafka/jaas.config",
        ],
    ),
    SchemaRegistry(
        environment_extra=[
            # Only allow this schema registry, which does not require
            # authentication, to be the leader. This simplifies testdrive, as
            # it is assured that it can submit requests to the schema registry
            # without needing authentication.
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=true",
        ]
    ),
    SchemaRegistry(
        name="schema-registry-basic",
        aliases=["basic.schema-registry.local"],
        environment_extra=[
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=false",
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES=user",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry",
            "SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/jaas.config",
        ],
        volumes=[
            "./schema-registry.jaas.config:/etc/schema-registry/jaas.config",
            "./schema-registry.user.properties:/etc/schema-registry/user.properties",
        ],
    ),
    SchemaRegistry(
        name="schema-registry-ssl",
        aliases=["ssl.schema-registry.local"],
        environment_extra=[
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=false",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081,https://0.0.0.0:8082",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
        ],
    ),
    SchemaRegistry(
        name="schema-registry-mssl",
        aliases=["mssl.schema-registry.local"],
        environment_extra=[
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=false",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081,https://0.0.0.0:8082",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_CLIENT_AUTH=true",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
        ],
    ),
    SchemaRegistry(
        name="schema-registry-ssl-basic",
        aliases=["ssl-basic.schema-registry.local"],
        environment_extra=[
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=false",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081,https://0.0.0.0:8082",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES=user",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry",
            "SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/jaas.config",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
            "./schema-registry.jaas.config:/etc/schema-registry/jaas.config",
            "./schema-registry.user.properties:/etc/schema-registry/user.properties",
        ],
    ),
    SchemaRegistry(
        name="schema-registry-mssl-basic",
        aliases=["mssl-basic.schema-registry.local"],
        environment_extra=[
            "SCHEMA_REGISTRY_LEADER_ELIGIBILITY=false",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081,https://0.0.0.0:8082",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_CLIENT_AUTH=true",
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES=user",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry",
            "SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/schema-registry/jaas.config",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
            "./schema-registry.jaas.config:/etc/schema-registry/jaas.config",
            "./schema-registry.user.properties:/etc/schema-registry/user.properties",
        ],
    ),
    Materialized(
        volumes_extra=["secrets:/share/secrets"],
    ),
    Testdrive(
        volumes_extra=["secrets:/share/secrets"],
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive against an authentication-enabled Confluent Platform."""

    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    args = parser.parse_args()

    # Bring up a single schema registry first, so that it can bootstrap the
    # underlying Kafka topic. Attempting to bring up multiple schema registries
    # simultaneously will cause several to fail to bootstrap the underlying
    # Kafka topic.
    c.up("ssh-bastion-host", "schema-registry", "materialized")

    # Add `materialize` SCRAM user to Kafka.
    c.exec(
        "kafka",
        "kafka-configs",
        "--bootstrap-server=localhost:9092",
        "--alter",
        "--add-config=SCRAM-SHA-256=[password=sekurity],SCRAM-SHA-512=[password=sekurity]",
        "--entity-type=users",
        "--entity-name=materialize",
    )

    # Restrict the `materialize_no_describe_configs` user from running the
    # `DescribeConfigs` cluster operation, but allow it to idempotently write
    # to all topics.
    c.exec(
        "kafka",
        "kafka-acls",
        "--bootstrap-server",
        "localhost:9092",
        "--add",
        "--cluster",
        "--deny-principal=User:materialize_no_describe_configs",
        "--operation=DescribeConfigs",
    )
    c.exec(
        "kafka",
        "kafka-acls",
        "--bootstrap-server",
        "localhost:9092",
        "--add",
        "--allow-principal=User:materialize_no_describe_configs",
        "--operation=ALL",
        "--transactional-id=*",
    )
    c.exec(
        "kafka",
        "kafka-acls",
        "--bootstrap-server",
        "localhost:9092",
        "--add",
        "--allow-principal=User:materialize_no_describe_configs",
        "--operation=ALL",
        "--topic=*",
    )

    # Now that the Kafka topic has been bootstrapped, it's safe to bring up all
    # the other schema registries in parallel.
    c.up(
        "schema-registry-basic",
        "schema-registry-ssl",
        "schema-registry-mssl",
        "schema-registry-ssl-basic",
        "schema-registry-mssl-basic",
    )

    # Set up SSH connection.
    c.sql(
        """
        CREATE DATABASE IF NOT EXISTS testdrive_no_reset_connections;
        CREATE CONNECTION IF NOT EXISTS testdrive_no_reset_connections.public.ssh TO SSH TUNNEL (
            HOST 'ssh-bastion-host',
            USER 'mz',
            PORT 22
        );
    """
    )
    public_key = c.sql_query(
        "select public_key_1 from mz_ssh_tunnel_connections where id = 'u1';"
    )[0][0]
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    # Set up backup SSH connection.
    c.sql(
        """
        CREATE DATABASE IF NOT EXISTS testdrive_no_reset_connections;
        CREATE CONNECTION IF NOT EXISTS testdrive_no_reset_connections.public.ssh_backup TO SSH TUNNEL (
            HOST 'ssh-bastion-host',
            USER 'mz',
            PORT 22
        );
    """
    )
    public_key = c.sql_query(
        "select public_key_1 from mz_ssh_tunnel_connections where id = 'u2';"
    )[0][0]
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' >> /etc/authorized_keys/mz",
    )

    c.run("testdrive", f"test-{args.filter}.td")
