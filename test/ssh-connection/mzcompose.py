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
    Materialized,
    Postgres,
    SshBastionHost,
    TestCerts,
    Testdrive,
)

SERVICES = [
    Materialized(),
    Testdrive(),
    SshBastionHost(),
    Postgres(),
    TestCerts(),
]


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()


def workflow_basic_ssh_features(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["materialized", "ssh-bastion-host", "postgres"])
    c.wait_for_materialized("materialized")

    c.run("testdrive", "ssh-connections.td")


# This test should also be executed with SSL enabled when it stops being flaky
def workflow_pg_via_ssh_tunnel(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["materialized", "ssh-bastion-host", "postgres"])
    c.wait_for_materialized("materialized")

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

    c.wait_for_postgres()

    c.run("testdrive", "--no-reset", "pg-source.td")


def workflow_ssh_key_after_restart(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized("materialized")

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
    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized("materialized")

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
    workflow_pg_via_ssh_tunnel(c)
    workflow_ssh_key_after_restart(c)
    workflow_rotated_ssh_key_after_restart(c)
