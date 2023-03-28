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
    Postgres,
    SchemaRegistry,
    SshBastionHost,
    TestCerts,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
    SshBastionHost(),
    Postgres(),
    TestCerts(),
]


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


# restart the bastion, wiping its keys in the process
def restart_bastion(c: Composition) -> None:
    c.kill("ssh-bastion-host")
    c.rm("ssh-bastion-host")
    c.up("ssh-bastion-host")


def workflow_basic_ssh_features(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run("testdrive", "ssh-connections.td")


def workflow_pg_via_ssh_tunnel(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run("testdrive", "setup.td")

    public_key = c.sql_query("select public_key_1 from mz_ssh_tunnel_connections;")[0][
        0
    ]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run("testdrive", "--no-reset", "pg-source.td")


def workflow_kafka(c: Composition) -> None:
    with c.override(Testdrive(consistent_seed=True)):
        c.up(
            "zookeeper", "kafka", "schema-registry", "materialized", "ssh-bastion-host"
        )

        c.run("testdrive", "setup.td")

        # Ensure we don't select the `keyless` key.
        thancred_id = c.sql_query(
            "select id from mz_connections where name = 'thancred';"
        )[0][0]
        public_key = c.sql_query(
            f"select public_key_1 from mz_ssh_tunnel_connections where id = '{thancred_id}';"
        )[0][0]

        c.exec(
            "ssh-bastion-host",
            "bash",
            "-c",
            f"echo '{public_key}' > /etc/authorized_keys/mz",
        )

        c.run("testdrive", "--no-reset", "kafka-source.td")
        c.run("testdrive", "--no-reset", "kafka-sink.td")

        c.kill("ssh-bastion-host")

        c.run("testdrive", "--no-reset", "kafka-after-ssh-fails.td")


# Test that if we restart the bastion AND change its server keys(s), we can
# still reconnect in the replication stream.
def workflow_pg_restart_bastion(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run("testdrive", "setup.td")

    public_key = c.sql_query("select public_key_1 from mz_ssh_tunnel_connections;")[0][
        0
    ]
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
        f"cat /etc/ssh/keys/ssh_host_ed25519_key.pub",
        capture=True,
    ).stdout.strip()

    c.run("testdrive", "--no-reset", "pg-source.td")

    restart_bastion(c)
    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run("testdrive", "--no-reset", "pg-source-ingest-more.td")

    # we do this after we assert that we re-connnected
    # with the passing td file, to ensure that the
    # docker image was setup before we actually start reading
    # stuff from it
    second_fingerprint = c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"cat /etc/ssh/keys/ssh_host_ed25519_key.pub",
        capture=True,
    ).stdout.strip()
    assert (
        first_fingerprint != second_fingerprint
    ), "this test requires that the ssh server fingerprint changes"


def workflow_pg_via_ssh_tunnel_with_ssl(c: Composition) -> None:
    c.up("materialized", "ssh-bastion-host", "postgres")

    c.run("testdrive", "setup.td")

    public_key = c.sql_query("SELECT public_key_1 FROM mz_ssh_tunnel_connections;")[0][
        0
    ]

    c.exec(
        "ssh-bastion-host",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    c.run("testdrive", "--no-reset", "pg-source-ssl.td")


def workflow_ssh_key_after_restart(c: Composition) -> None:
    c.up("materialized")

    c.run("testdrive", "setup.td")

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
    if num_connections != 0:
        connections = c.sql_query("SELECT * FROM mz_ssh_tunnel_connections;")
        print("Found connections in mz_ssh_tunnel_connections: ", connections)
        raise Exception(
            "ssh tunnel connection not properly removed from mz_ssh_tunnel_connections"
        )


def workflow_rotated_ssh_key_after_restart(c: Composition) -> None:
    c.up("materialized")

    c.run("testdrive", "setup.td")

    secondary_public_key = c.sql_query(
        "SELECT public_key_2 FROM mz_ssh_tunnel_connections;"
    )[0][0]

    c.sql("ALTER CONNECTION thancred ROTATE KEYS;")

    restart_mz(c)

    primary_public_key_after_restart = c.sql_query(
        "SELECT public_key_1 FROM mz_ssh_tunnel_connections;"
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
    if num_connections != 0:
        connections = c.sql_query("SELECT * FROM mz_ssh_tunnel_connections;")
        print("Found connections in mz_ssh_tunnel_connections: ", connections)
        raise Exception(
            "ssh tunnel connection not properly removed from mz_ssh_tunnel_connections after key rotation"
        )


def workflow_default(c: Composition) -> None:
    workflow_basic_ssh_features(c)
    workflow_kafka(c)
    workflow_ssh_key_after_restart(c)
    workflow_rotated_ssh_key_after_restart(c)
    workflow_pg_via_ssh_tunnel(c)
    workflow_pg_via_ssh_tunnel_with_ssl(c)
    workflow_pg_restart_bastion(c)
