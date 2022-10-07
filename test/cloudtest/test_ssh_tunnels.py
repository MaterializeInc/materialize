# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def test_ssh_tunnels(mz: MaterializeApplication) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > CREATE CONNECTION IF NOT EXISTS ssh_conn
                FOR SSH TUNNEL
                    HOST 'ssh-bastion-host',
                    USER 'mz',
                    PORT 22;
            """
        )
    )

    (id, public_key) = mz.environmentd.sql_query(
        "SELECT id, public_key_1 FROM mz_ssh_tunnel_connections"
    )[0]
    assert id is not None

    secret = f"user-managed-{id}"

    # If the secret didn't exist, this would throw an exception
    mz.kubectl("describe", "secret", secret)

    # Add public key to SSH bastion host
    mz.kubectl(
        "exec",
        "svc/ssh-bastion-host",
        "--",
        "bash",
        "-c",
        f"echo '{public_key}' > /etc/authorized_keys/mz",
    )

    mz.testdrive.run(
        input=dedent(
            """
        > CREATE SECRET pgpass AS 'postgres'
        > CREATE CONNECTION pg FOR POSTGRES
          HOST 'postgres-source',
          DATABASE postgres,
          USER postgres,
          PASSWORD SECRET pgpass,
          SSL MODE require,
          SSH TUNNEL ssh_conn

        $ postgres-execute connection=postgres://postgres:postgres@postgres-source
        ALTER USER postgres WITH replication;
        DROP SCHEMA IF EXISTS public CASCADE;
        DROP PUBLICATION IF EXISTS mz_source;
        CREATE SCHEMA public;

        CREATE TABLE t1 (f1 INTEGER);
        ALTER TABLE t1 REPLICA IDENTITY FULL;
        INSERT INTO t1 VALUES (1);

        CREATE PUBLICATION mz_source FOR TABLE t1;

        > CREATE SOURCE mz_source
          FROM POSTGRES CONNECTION pg
          (PUBLICATION 'mz_source')
          FOR ALL TABLES;

        > SELECT COUNT(*) = 1 FROM t1;
        true

        > SELECT f1 FROM t1;
        1

        $ postgres-execute connection=postgres://postgres:postgres@postgres-source
        INSERT INTO t1 VALUES (1), (2);

        > SELECT f1 FROM t1 ORDER BY f1 ASC;
        1
        1
        2
        """
        ),
        no_reset=True,
    )

    environmentd_pod_name = f"pod/environmentd-0"

    # Kill environmentd to force a restart, to test reloading secrets on restart
    mz.kubectl(
        "exec",
        environmentd_pod_name,
        "--",
        "bash",
        "-c",
        "kill -9 `pidof environmentd`",
    )

    mz.testdrive.run(
        input=dedent(
            """
        > SELECT f1 FROM t1 ORDER BY f1 ASC;
        1
        1
        2

        $ postgres-execute connection=postgres://postgres:postgres@postgres-source
        INSERT INTO t1 VALUES (3), (4);

        > SELECT f1 FROM t1 ORDER BY f1 ASC;
        1
        1
        2
        3
        4
        """
        ),
        no_reset=True,
    )

    mz.environmentd.sql("DROP CONNECTION ssh_conn CASCADE")

    # Verify that secret associated with the SSH tunnel is deleted from k8s
    wait(condition="delete", resource=f"secret/{secret}")
