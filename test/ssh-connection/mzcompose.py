# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test sources with SSH connections using an SSH bastion host.
"""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(consistent_seed=True),
    SshBastionHost(),
    Postgres(),
    TestCerts(),
    Redpanda(),
    MySql(),
]


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


# restart the bastion, wiping its keys in the process
def restart_bastion(c: Composition) -> None:
    c.kill("ssh-bastion-host")
    c.rm("ssh-bastion-host")
    c.up("ssh-bastion-host")


def workflow_basic_ssh_features(c: Composition, redpanda: bool = False) -> None:
    c.down()

    dependencies = ["materialized", "ssh-bastion-host"]
    if redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]
    c.up(*dependencies)

    c.run_testdrive_files("ssh-connections.td")

    # Check that objects can be restored correctly
    restart_mz(c)


def workflow_validate_connection(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.run_testdrive_files("--no-reset", "validate-failures.td")

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run_testdrive_files("--no-reset", "validate-success.td")


def workflow_pg(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run_testdrive_files("--no-reset", "pg-source.td")
    c.kill("ssh-bastion-host")
    c.run_testdrive_files("--no-reset", "pg-source-after-ssh-failure.td")
    c.up("ssh-bastion-host")
    c.run_testdrive_files("--no-reset", "pg-source-after-ssh-restart.td")


def workflow_mysql(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "mysql")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    # Basic validation
    c.run_testdrive_files(
        "--no-reset",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql-source.td",
    )

    # Validate SSH bastion host failure & recovery scenario
    c.kill("ssh-bastion-host")
    c.run_testdrive_files("--no-reset", "mysql-source-after-ssh-failure.td")
    c.up("ssh-bastion-host")
    c.run_testdrive_files(
        "--no-reset",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql-source-after-ssh-restart.td",
    )

    # MySQL generates self-signed certificates for SSL connections on startup,
    # for both the server and client:
    # https://dev.mysql.com/doc/refman/8.3/en/creating-ssl-rsa-files-using-mysql.html
    # Grab the correct Server CA and Client Key and Cert from the MySQL container
    # (and strip the trailing null byte):
    ssl_ca = c.exec("mysql", "cat", "/var/lib/mysql/ca.pem", capture=True).stdout.split(
        "\x00", 1
    )[0]
    ssl_client_cert = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-cert.pem", capture=True
    ).stdout.split("\x00", 1)[0]
    ssl_client_key = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-key.pem", capture=True
    ).stdout.split("\x00", 1)[0]

    # Validate SSL/TLS connections over SSH tunnel
    c.run_testdrive_files(
        "--no-reset",
        f"--var=ssl-ca={ssl_ca}",
        f"--var=ssl-client-cert={ssl_client_cert}",
        f"--var=ssl-client-key={ssl_client_key}",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql-source-ssl.td",
    )


def workflow_kafka(c: Composition, redpanda: bool = False) -> None:
    c.down()
    # Configure the SSH bastion host to allow only two connections to be
    # initiated simultaneously. This is enough to establish *one* Kafka SSH
    # tunnel and *one* Confluent Schema Registry tunnel simultaneously.
    # Combined with using a large cluster in kafka-source.td, this ensures that
    # we only create one SSH tunnel per Kafka broker, rather than one SSH tunnel
    # per Kafka broker per worker.
    with c.override(SshBastionHost(max_startups="2")):

        dependencies = ["materialized", "ssh-bastion-host"]
        if redpanda:
            dependencies += ["redpanda"]
        else:
            dependencies += ["zookeeper", "kafka", "schema-registry"]
        c.up(*dependencies)

        c.run_testdrive_files("setup.td")

        public_key = c.sql_query(
            """
            select public_key_1 from mz_ssh_tunnel_connections ssh \
            join mz_connections c on c.id = ssh.id
            where c.name = 'thancred';
            """
        )[0][0]

        c.exec(
            "ssh-bastion-host",
            "bash",
            "-c",
            f"echo '{public_key}' > /etc/authorized_keys/mz",
        )

        c.run_testdrive_files("--no-reset", "kafka-source.td")
        c.kill("ssh-bastion-host")
        c.run_testdrive_files("--no-reset", "kafka-source-after-ssh-failure.td")

        c.up("ssh-bastion-host")
        c.run_testdrive_files("--no-reset", "kafka-source-after-ssh-restart.td")


def workflow_kafka_restart_replica(c: Composition, redpanda: bool = False) -> None:
    c.down()
    # Configure the SSH bastion host to allow only two connections to be
    # initiated simultaneously. This is enough to establish *one* Kafka SSH
    # tunnel and *one* Confluent Schema Registry tunnel simultaneously.
    # Combined with using a large cluster in kafka-source.td, this ensures that
    # we only create one SSH tunnel per Kafka broker, rather than one SSH tunnel
    # per Kafka broker per worker.
    with c.override(SshBastionHost(max_startups="2")):

        dependencies = ["materialized", "ssh-bastion-host"]
        if redpanda:
            dependencies += ["redpanda"]
        else:
            dependencies += ["zookeeper", "kafka", "schema-registry"]
        c.up(*dependencies)

        c.run_testdrive_files("setup.td")

        public_key = c.sql_query(
            """
            select public_key_1 from mz_ssh_tunnel_connections ssh \
            join mz_connections c on c.id = ssh.id
            where c.name = 'thancred';
            """
        )[0][0]

        c.exec(
            "ssh-bastion-host",
            "bash",
            "-c",
            f"echo '{public_key}' > /etc/authorized_keys/mz",
        )

        c.run_testdrive_files("--no-reset", "kafka-source.td")
        c.kill("ssh-bastion-host")
        c.run_testdrive_files(
            "--no-reset",
            "kafka-source-after-ssh-failure-restart-replica.td",
        )

        c.up("ssh-bastion-host")
        c.run_testdrive_files("--no-reset", "kafka-source-after-ssh-restart.td")


def workflow_kafka_sink(c: Composition, redpanda: bool = False) -> None:
    c.down()
    # Configure the SSH bastion host to allow only two connections to be
    # initiated simultaneously. This is enough to establish *one* Kafka SSH
    # tunnel and *one* Confluent Schema Registry tunnel simultaneously.
    # Combined with using a large cluster in kafka-source.td, this ensures that
    # we only create one SSH tunnel per Kafka broker, rather than one SSH tunnel
    # per Kafka broker per worker.
    with c.override(SshBastionHost(max_startups="2")):

        dependencies = ["materialized", "ssh-bastion-host"]
        if redpanda:
            dependencies += ["redpanda"]
        else:
            dependencies += ["zookeeper", "kafka", "schema-registry"]
        c.up(*dependencies)

        c.run_testdrive_files("setup.td")

        public_key = c.sql_query(
            """
            select public_key_1 from mz_ssh_tunnel_connections ssh \
            join mz_connections c on c.id = ssh.id
            where c.name = 'thancred';
            """
        )[0][0]

        c.exec(
            "ssh-bastion-host",
            "bash",
            "-c",
            f"echo '{public_key}' > /etc/authorized_keys/mz",
        )

        c.run_testdrive_files("--no-reset", "kafka-sink.td")
        c.kill("ssh-bastion-host")
        c.run_testdrive_files("--no-reset", "kafka-sink-after-ssh-failure.td")

        c.up("ssh-bastion-host")
        c.run_testdrive_files("--no-reset", "kafka-sink-after-ssh-restart.td")


def workflow_hidden_hosts(c: Composition, redpanda: bool = False) -> None:
    c.down()
    dependencies = ["materialized", "ssh-bastion-host"]
    if redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]
    c.up(*dependencies)

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    def add_hidden_host(container: str) -> None:
        ip = c.exec(
            "ssh-bastion-host", "getent", "hosts", container, capture=True
        ).stdout.split(" ")[0]
        c.exec(
            "ssh-bastion-host",
            "bash",
            "-c",
            f"echo '{ip} hidden-{container}' >> /etc/hosts",
        )

    add_hidden_host("kafka")
    add_hidden_host("schema-registry")

    c.run_testdrive_files("--no-reset", "hidden-hosts.td")


# Test that if we restart the bastion AND change its server keys(s), we can
# still reconnect in the replication stream.
def workflow_pg_restart_bastion(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )
    first_fingerprint = c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        "cat /etc/ssh/keys/ssh_host_ed25519_key.pub",
        capture=True,
    ).stdout.strip()

    c.run_testdrive_files("--no-reset", "pg-source.td")

    restart_bastion(c)
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run_testdrive_files("--no-reset", "pg-source-ingest-more.td")

    # we do this after we assert that we re-connnected
    # with the passing td file, to ensure that the
    # docker image was setup before we actually start reading
    # stuff from it
    second_fingerprint = c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        "cat /etc/ssh/keys/ssh_host_ed25519_key.pub",
        capture=True,
    ).stdout.strip()
    assert (
        first_fingerprint != second_fingerprint
    ), "this test requires that the ssh server fingerprint changes"


def workflow_pg_restart_postgres(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run_testdrive_files("--no-reset", "pg-source.td")

    c.kill("postgres")
    c.up("postgres")

    c.run_testdrive_files("--no-reset", "pg-source-ingest-more.td")


def workflow_pg_via_ssh_tunnel_with_ssl(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run_testdrive_files("setup.td")

    public_key = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run_testdrive_files("--no-reset", "pg-source-ssl.td")


def workflow_ssh_key_after_restart(c: Composition) -> None:
    c.up("materialized")

    c.run_testdrive_files("setup.td")

    (primary, secondary) = c.sql_query(
        "SELECT public_key_1, public_key_2 FROM mz_ssh_tunnel_connections;"
    )[0]

    restart_mz(c)

    (restart_primary, restart_secondary) = c.sql_query(
        "SELECT public_key_1, public_key_2 FROM mz_ssh_tunnel_connections;"
    )[0]

    if (primary, secondary) != (restart_primary, restart_secondary):
        print("initial public keys: ", (primary, secondary))
        print("public keys after restart:", (restart_primary, restart_secondary))
        raise Exception("public key not equal after restart")

    c.sql("DROP CONNECTION thancred;")
    num_connections = c.sql_query("SELECT count(*) FROM mz_ssh_tunnel_connections;")[0][
        0
    ]
    if num_connections != 1:
        connections = c.sql_query("SELECT * FROM mz_ssh_tunnel_connections;")
        print("Found connections in mz_ssh_tunnel_connections: ", connections)
        raise Exception(
            "ssh tunnel connection not properly removed from mz_ssh_tunnel_connections"
        )


def workflow_rotated_ssh_key_after_restart(c: Composition) -> None:
    c.up("materialized")

    c.run_testdrive_files("setup.td")

    secondary_public_key = c.sql_query(
        """
        select public_key_2 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    c.sql("ALTER CONNECTION thancred ROTATE KEYS;")

    restart_mz(c)

    primary_public_key_after_restart = c.sql_query(
        """
        select public_key_1 from mz_ssh_tunnel_connections ssh \
        join mz_connections c on c.id = ssh.id
        where c.name = 'thancred';
        """
    )[0][0]

    if secondary_public_key != primary_public_key_after_restart:
        print("initial secondary key:", secondary_public_key)
        print(
            "primary public key after rotation + restart:",
            primary_public_key_after_restart,
        )
        raise Exception("public keys don't match")

    c.sql("DROP CONNECTION thancred;")
    num_connections = c.sql_query("SELECT count(*) FROM mz_ssh_tunnel_connections;")[0][
        0
    ]
    if num_connections != 1:
        connections = c.sql_query("SELECT * FROM mz_ssh_tunnel_connections;")
        print("Found connections in mz_ssh_tunnel_connections: ", connections)
        raise Exception(
            "ssh tunnel connection not properly removed from mz_ssh_tunnel_connections after key rotation"
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--extended",
        action="store_true",
        help="run additional tests",
    )
    args = parser.parse_args()

    # Test against both standard schema registry
    # and kafka implementations, if --extended is passed
    #
    # These tests core functionality related to kafka with ssh and error reporting.
    for workflow in [
        workflow_kafka,
        workflow_hidden_hosts,
    ]:
        workflow(c, redpanda=False)
        c.sanity_restart_mz()
        c.run_testdrive_files("--no-reset", "validate-success.td")
        if args.extended:
            workflow(c, redpanda=True)
            c.sanity_restart_mz()
            c.run_testdrive_files("--no-reset", "validate-success.td")

    # These tests core functionality related to pg with ssh and error reporting.
    workflow_basic_ssh_features(c)
    c.sanity_restart_mz()
    for workflow in [
        workflow_pg,
        workflow_kafka_restart_replica,
        workflow_kafka_sink,
    ]:
        workflow(c)
        c.sanity_restart_mz()
        c.run_testdrive_files("--no-reset", "validate-success.td")

    if args.extended:
        # Various special cases related to ssh
        for workflow in [
            workflow_ssh_key_after_restart,
            workflow_rotated_ssh_key_after_restart,
        ]:
            workflow(c)
            c.sanity_restart_mz()

        for workflow in [
            workflow_validate_connection,
            workflow_pg_via_ssh_tunnel_with_ssl,
            workflow_pg_restart_bastion,
            workflow_pg_restart_postgres,
        ]:
            workflow(c)
            c.sanity_restart_mz()
            c.run_testdrive_files("--no-reset", "validate-success.td")
