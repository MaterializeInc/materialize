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
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def test_secrets(mz: MaterializeApplication) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > CREATE SECRET username AS '123';
            > CREATE SECRET password AS '234';

            > CREATE CONNECTION secrets_conn TO KAFKA (
                BROKER '${testdrive.kafka-addr}',
                SASL MECHANISMS 'PLAIN',
                SASL USERNAME = SECRET username,
                SASL PASSWORD = SECRET password
              );

            # Our Redpanda instance is not configured for SASL, so we can not
            # really establish a successful connection. Hence the expectation for an SSL error
            ! CREATE SOURCE secrets_source
              FROM KAFKA CONNECTION secrets_conn (TOPIC 'foo_bar');
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


# Tests that secrets deleted from the catalog but not from k8s are cleaned up on
# envd startup.
@pytest.mark.skip(reason="Failpoints mess up the Mz intance #18000")
def test_orphaned_secrets(mz: MaterializeApplication) -> None:
    # Use two separate failpoints. One that crashes after modifying the catalog
    # (drop_secrets), and one that fails during bootstrap (orphan_secrets) so
    # that we can prevent a racy startup from cleaning up the secret before we
    # observed it.
    mz.set_environmentd_failpoints("orphan_secrets=panic")
    mz.environmentd.sql("SET failpoints = 'drop_secrets=panic'")
    mz.environmentd.sql("CREATE SECRET orphan AS '123'")

    id = mz.environmentd.sql_query("SELECT id FROM mz_secrets WHERE name = 'orphan'")[
        0
    ][0]
    assert id is not None
    secret = f"user-managed-{id}"

    # The failpoint should cause this to fail.
    try:
        mz.environmentd.sql("DROP SECRET orphan")
        raise Exception("Unexpected success")
    except InterfaceError:
        pass

    describe = mz.kubectl("describe", "secret", secret)
    assert "contents:  3 bytes" in describe

    # We saw the secret, allow orphan cleanup.
    mz.set_environmentd_failpoints("")

    mz.wait_for_sql()
    wait(condition="delete", resource=f"secret/{secret}")
