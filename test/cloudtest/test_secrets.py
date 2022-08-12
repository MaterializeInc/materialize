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


def test_secrets(mz: MaterializeApplication) -> None:
    mz.testdrive.run_string(
        dedent(
            """
            > CREATE SECRET username AS '123';
            > CREATE SECRET password AS '234';

            # Our Redpanda instance is not configured for SASL, so we can not
            # really establish a successful connection.

            ! CREATE CONNECTION secrets_conn
              FOR KAFKA
              BROKER BROKER '${testdrive.kafka-addr}',
              SASL MECHANISMS 'PLAIN',
              SASL USERNAME = SECRET username,
              SASL PASSWORD = SECRET passsword;

            contains: SSL handshake failed
            """
        )
    )

    id = mz.environmentd.sql_query("SELECT id FROM mz_secrets WHERE name = 'username'")[
        0
    ][0]
    assert id is not None

    secret = f"user-managed-{id}"

    #    wait(condition="condition=Ready", resource=f"secret/{secret}")

    describe = mz.kubectl("describe", "secret", secret)
    assert "contents:  3 bytes" in describe

    mz.environmentd.sql("ALTER SECRET username AS '1234567890'")

    describe = mz.kubectl("describe", "secret", secret)
    assert "contents:  10 bytes" in describe

    mz.environmentd.sql("DROP SECRET username CASCADE")

    wait(condition="delete", resource=f"secret/{secret}")
