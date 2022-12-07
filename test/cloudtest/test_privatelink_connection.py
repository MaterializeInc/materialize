# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

import pytest
from pg8000.dbapi import ProgrammingError

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.exists import exists, not_exists


def test_create_privatelink_connection(mz: MaterializeApplication) -> None:
    # Create a PrivateLink SQL connection object,
    # which should create a K8S VpcEndpoint object.
    # We don't run the environment-controller,
    # so no AWS VPC Endpoint will be created.
    # so we don't need the named service to actually exist.
    create_connection_statement = dedent(
        """\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az4')
        )
        """
    )

    # This should fail until max_aws_privatelink_connections is increased.
    with pytest.raises(
        ProgrammingError,
        match="AWS PrivateLink Connection resource limit of 0 cannot be exceeded",
    ):
        mz.environmentd.sql(create_connection_statement)

    next_gid = mz.environmentd.sql_query(
        "SELECT MAX(SUBSTR(id, 2, LENGTH(id) - 1)::int) + 1 FROM mz_objects WHERE id LIKE 'u%'"
    )[0][0]

    not_exists(resource=f"vpcendpoint/connection-u{next_gid}")

    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    mz.environmentd.sql(create_connection_statement)

    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]

    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")

    # TODO: validate the contents of the VPC endpoint resource, rather than just
    # its existence.

    mz.environmentd.sql(
        dedent(
            """\
            CREATE CONNECTION kafkaconn TO KAFKA (
                BROKERS (
                    'customer-hostname-1:9092' USING AWS PRIVATELINK privatelinkconn,
                    'customer-hostname-2:9092' USING AWS PRIVATELINK privatelinkconn (PORT 9093),
                    'customer-hostname-4:9094'
                )
            );
            """
        )
    )
    mz.environmentd.sql_query("SELECT id FROM mz_connections WHERE name = 'kafkaconn'")[
        0
    ][0]

    principal = mz.environmentd.sql_query(
        "SELECT principal FROM mz_aws_privatelink_connections"
    )[0][0]

    assert principal == (
        f"arn:aws:iam::123456789000:role/mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_{aws_connection_id}"
    )

    mz.environmentd.sql(
        dedent(
            """\
            CREATE CONNECTION sshconn TO SSH TUNNEL (
                HOST 'ssh-bastion-host',
                USER 'mz',
                PORT 22
            );
            """
        )
    )
    with pytest.raises(
        ProgrammingError, match="cannot specify both SSH TUNNEL and AWS PRIVATELINK"
    ):
        mz.environmentd.sql(
            dedent(
                """\
            CREATE CONNECTION pg TO POSTGRES (
                HOST 'postgres-source',
                DATABASE postgres,
                USER postgres,
                AWS PRIVATELINK privatelinkconn,
                SSH TUNNEL sshconn
            )
            """
            )
        )

    mz.environmentd.sql("DROP CONNECTION kafkaconn")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn")

    not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}")
