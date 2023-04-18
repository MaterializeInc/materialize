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
from materialize.cloudtest.k8s import cluster_pod_name
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
            # really establish a successful connection.
            ! CREATE SOURCE secrets_source
              FROM KAFKA CONNECTION secrets_conn (TOPIC 'foo_bar');
            contains:Meta data fetch error: BrokerTransportFailure (Local: Broker transport failure)
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


def test_missing_secret(mz: MaterializeApplication) -> None:
    """Test that Mz does not panic if a secret goes missing from K8s"""
    mz.testdrive.run(
        input=dedent(
            """
          > CREATE CLUSTER to_be_killed REPLICAS (to_be_killed (SIZE '1'));

          > CREATE SECRET to_be_deleted AS 'postgres'

          > CREATE CONNECTION kafka_conn_with_deleted_secret TO KAFKA (
                BROKER '${testdrive.kafka-addr}',
                SASL MECHANISMS 'PLAIN',
                SASL USERNAME = SECRET to_be_deleted,
                SASL PASSWORD = SECRET to_be_deleted
              );

          > CREATE CONNECTION pg_conn_with_deleted_secret TO POSTGRES (
            HOST 'postgres',
            DATABASE postgres,
            USER postgres,
            PASSWORD SECRET to_be_deleted
            );

          $ postgres-execute connection=postgres://postgres:postgres@postgres
          ALTER USER postgres WITH replication;
          DROP SCHEMA IF EXISTS public CASCADE;
          DROP PUBLICATION IF EXISTS mz_source;
          CREATE SCHEMA public;

          CREATE TABLE t1 (f1 INTEGER);
          ALTER TABLE t1 REPLICA IDENTITY FULL;
          INSERT INTO t1 VALUES (1);

          CREATE PUBLICATION mz_source FOR TABLE t1;

          > CREATE SOURCE source_with_deleted_secret
            IN CLUSTER to_be_killed
            FROM POSTGRES CONNECTION pg_conn_with_deleted_secret
            (PUBLICATION 'mz_source')
            FOR ALL TABLES;
     """
        )
    )

    id = mz.environmentd.sql_query(
        "SELECT id FROM mz_secrets WHERE name = 'to_be_deleted'"
    )[0][0]
    assert id is not None
    secret = f"user-managed-{id}"

    mz.kubectl("delete", "secret", secret)
    wait(condition="delete", resource=f"secret/{secret}")

    mz.testdrive.run(
        input=dedent(
            """
            ! CREATE SOURCE some_pg_source
              FROM POSTGRES CONNECTION pg_conn_with_deleted_secret
              (PUBLICATION 'mz_source')
              FOR ALL TABLES;
            contains: NotFound

            ! CREATE SOURCE some_kafka_source
              FROM KAFKA CONNECTION kafka_conn_with_deleted_secret
              (TOPIC 'foo')
            contains: NotFound
            """
        ),
        no_reset=True,
    )

    # Restart the storage computed and confirm that the source errors out properly

    cluster_id, replica_id = mz.environmentd.sql_query(
        f"SELECT cluster_id, id FROM mz_cluster_replicas WHERE name = 'to_be_killed'"
    )[0]
    pod_name = cluster_pod_name(cluster_id, replica_id, 0)

    mz.kubectl("exec", pod_name, "--", "bash", "-c", f"kill -9 `pidof clusterd`")
    wait(condition="condition=Ready", resource=f"{pod_name}")

    mz.testdrive.run(
        input=dedent(
            """
            ! CREATE SOURCE some_pg_source
              FROM POSTGRES CONNECTION pg_conn_with_deleted_secret
              (PUBLICATION 'mz_source')
              FOR ALL TABLES;
            contains: NotFound

            ! CREATE SOURCE some_kafka_source
              FROM KAFKA CONNECTION kafka_conn_with_deleted_secret
              (TOPIC 'foo')
            contains: NotFound

            > SELECT error like '%NotFound%'
              FROM mz_internal.mz_source_statuses
              WHERE name = 'source_with_deleted_secret';
            true
            """
        ),
        no_reset=True,
    )

    # Kill the environmentd and confirm the same

    mz.kubectl(
        "exec",
        "pod/environmentd-0",
        "--",
        "bash",
        "-c",
        f"kill -9 `pidof environmentd`",
    )
    wait(condition="condition=Ready", resource=f"pod/environmentd-0")

    mz.testdrive.run(
        input=dedent(
            """
            ! CREATE SOURCE some_pg_source
              FROM POSTGRES CONNECTION pg_conn_with_deleted_secret
              (PUBLICATION 'mz_source')
              FOR ALL TABLES;
            contains: NotFound

            ! CREATE SOURCE some_kafka_source
              FROM KAFKA CONNECTION kafka_conn_with_deleted_secret
              (TOPIC 'foo')
            contains: NotFound

            > SELECT error like '%NotFound%'
              FROM mz_internal.mz_source_statuses
              WHERE name = 'source_with_deleted_secret';
            true

            ! DROP CLUSTER to_be_killed CASCADE;
            contains:error creating Postgres client for dropping acquired slots

            # The cluster should still be there.
            > SELECT name from mz_clusters where name = 'to_be_killed';
            to_be_killed

            # Try and put the secret in place again.
            > ALTER SECRET to_be_deleted AS 'postgres';

            # Cluster can now be deleted.
            > DROP CLUSTER to_be_killed CASCADE;
            """
        ),
        no_reset=True,
    )
