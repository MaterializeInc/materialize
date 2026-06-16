# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from textwrap import dedent
from typing import Any

from pg8000 import Connection
from pg8000.exceptions import DatabaseError

from materialize.cloudtest.app.materialize_application import (
    LOGGER,
    MaterializeApplication,
)
from materialize.cloudtest.util.cluster import cluster_pod_name
from materialize.cloudtest.util.wait import wait


def test_managed_cluster_sizing(mz: MaterializeApplication) -> None:
    """Test that a SIZE N cluster indeed creates N clusterd instances."""
    SIZE = 2

    mz.environmentd.sql(
        f"CREATE CLUSTER sized1 SIZE 'scale={SIZE},workers=1', REPLICATION FACTOR 2"
    )
    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert cluster_id is not None

    check = mz.environmentd.sql_query(
        "SELECT availability_zones IS NULL FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert check is not None
    assert check == True

    # Setting the availability-zone pool is a config-shape change. With the
    # cluster controller on it is realized as a graceful reconfiguration. The new
    # pool takes effect only once replacement replicas have hydrated and the
    # controller cuts over, so the realized `availability_zones` and the steady
    # replica set settle asynchronously; with the controller off the change is
    # applied synchronously in place. Poll (testdrive retries each query) for the
    # settled state so this passes either way. The cut-over does not preserve
    # replica names, so assert on replica *count* rather than specific names.
    mz.testdrive.run(
        input=dedent("""
            > ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('1', '2', '3'))

            > SELECT list_length(availability_zones) FROM mz_clusters WHERE name = 'sized1'
            3

            > SELECT count(*)
              FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id
              WHERE c.name = 'sized1'
            2
            """),
        no_reset=True,
    )

    mz.testdrive.run(
        input=dedent("""
            ! ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('4'))
            exact:unknown cluster replica availability zone 4
            """),
        no_reset=True,
    )

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert len(replicas) == SIZE

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    # Reducing the replication factor drops a replica: the controller does this on
    # its next reconcile tick (asynchronously), the legacy path synchronously.
    # Poll for the settled count.
    mz.testdrive.run(
        input=dedent("""
            > ALTER CLUSTER sized1 SET (REPLICATION FACTOR 1)

            > SELECT count(*)
              FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id
              WHERE c.name = 'sized1'
            1
            """),
        no_reset=True,
    )

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert len(replicas) == 1

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")

    mz.testdrive.run(
        input=dedent("""
            ! CREATE CLUSTER sizedbad (SIZE="badsize")
            contains:unknown cluster replica size badsize
            """),
        no_reset=True,
    )

    mz.environmentd.sql(
        'ALTER SYSTEM SET ALLOWED_CLUSTER_REPLICA_SIZES="scale=1,workers=1"',
        port="internal",
        user="mz_system",
    )
    try:
        mz.environmentd.sql(
            'CREATE CLUSTER mzsizetest (SIZE="scale=1,workers=2")',
            port="internal",
            user="mz_system",
        )

        mz.environmentd.sql(
            "DROP CLUSTER mzsizetest CASCADE",
            port="internal",
            user="mz_system",
        )
    finally:
        mz.environmentd.sql(
            "ALTER SYSTEM RESET ALLOWED_CLUSTER_REPLICA_SIZES",
            port="internal",
            user="mz_system",
        )


def test_zero_downtime_reconfiguration(mz: MaterializeApplication) -> None:
    # Drive the controller tick down so background reconfigurations converge
    # within the short poll loops below.
    mz.environmentd.sql(
        """
        ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true;
        ALTER SYSTEM SET cluster_controller_tick_interval = '5ms';
        """,
        port="internal",
        user="mz_system",
    )

    def get_replica_names(cluster: str) -> list[str]:
        replicas = mz.environmentd.sql_query(f"""
            SELECT mz_cluster_replicas.name
            FROM mz_cluster_replicas, mz_clusters
            WHERE mz_cluster_replicas.cluster_id = mz_clusters.id
            AND mz_clusters.name = '{cluster}';
            """)
        return sorted(replica[0] for replica in replicas)

    def assert_no_pending(cluster: str) -> None:
        # The controller never creates a legacy "-pending" replica.
        pending = mz.environmentd.sql_query(f"""
            SELECT cr.name
            FROM mz_internal.mz_pending_cluster_replicas pr
            INNER JOIN mz_cluster_replicas cr ON cr.id = pr.id
            INNER JOIN mz_clusters c ON c.id = cr.cluster_id
            WHERE c.name = '{cluster}';
            """)
        assert (
            len(pending) == 0
        ), f"There should be no pending replicas, found {pending}"

    def wait_for_replica_names(names: list[str], cluster: str = "zdtaltertest"):
        """Poll until the cluster's replica set settles on `names`.

        A background ALTER returns immediately and the controller converges the
        replica set on its tick, so readbacks have to wait for the settled
        state.
        """
        observed = None
        for _ in range(240):
            observed = get_replica_names(cluster)
            if observed == sorted(names):
                break
            time.sleep(0.25)
        assert observed == sorted(
            names
        ), f"expected replicas {names} on {cluster}, found {observed}"
        assert_no_pending(cluster)

    # Basic zero-downtime reconfig test cases matrix
    # - size change, no replica change
    # - size down, with replication factor up
    # - replication factor down, no other change
    # - size up, with replication factor up
    # - size down, with replication factor down
    # Other assertions
    # - replica names churn across reshapes: the controller realizes a
    #   config-shape change by bringing up freshly-named target replicas and
    #   dropping the old set at cut-over, never renaming back
    # - no pending replicas at any point
    # - a session cancel does not abort a durable background reconfiguration
    # - an ON TIMEOUT ROLLBACK reconfiguration that cannot hydrate in time
    #   rolls back in the background, leaving the realized config untouched
    # - fails to alter replication factor beyond max_replicas_per_cluster
    mz.environmentd.sql(
        'CREATE CLUSTER zdtaltertest ( SIZE = "scale=1,workers=1" )',
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r1"])

    # A size change reshapes in the background: a fresh target replica comes
    # up, the realized config cuts over, and the old replica is dropped
    # (r1 -> r2).
    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=2' ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r2"])
    assert (
        mz.environmentd.sql_query("""
        SELECT size FROM mz_clusters WHERE name='zdtaltertest';
        """) == (["scale=1,workers=2"],)
    ), "Realized size should have cut over to the target"

    # A size change with a replication-factor increase brings up two fresh
    # target replicas.
    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r3", "r4"])

    # A replication-factor-only decrease reconciles in place (no reshape): the
    # oldest replica is kept.
    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r3"])

    # Fresh names continue past the highest index ever observed.
    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=2', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r4", "r5"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r6"])

    # Setup for validating cancelation and
    # replica checks during alter
    mz.testdrive.run(
        no_reset=True,
        input=dedent("""
        $ kafka-create-topic topic=zdt-reconfig

        $ kafka-ingest topic=zdt-reconfig format=bytes key-format=bytes key-terminator=: repeat=1000
        key${kafka-ingest.iteration}:value${kafka-ingest.iteration}

        $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
        DROP CLUSTER IF EXISTS zdtaltertest CASCADE;
        DROP TABLE IF EXISTS t CASCADE;
        CREATE CLUSTER zdtaltertest ( SIZE = 'scale=1,workers=1');
        GRANT ALL ON CLUSTER zdtaltertest TO materialize;

        SET CLUSTER = zdtaltertest;

        > CREATE TABLE t (a int);
        > CREATE DEFAULT INDEX ON t;
        > INSERT INTO t VALUES (42);

        > CREATE CONNECTION kafka_conn
          TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

        > CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
            URL '${testdrive.schema-registry-url}'
          )

        > CREATE SOURCE kafka_src
          IN CLUSTER zdtaltertest
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-zdt-reconfig-${testdrive.seed}')

        > CREATE TABLE kafka_tbl
          FROM SOURCE kafka_src (REFERENCE "testdrive-zdt-reconfig-${testdrive.seed}")
          KEY FORMAT TEXT
          VALUE FORMAT TEXT
          ENVELOPE UPSERT
        """),
    )

    # Validate replicas are correct during an ongoing alter. The background
    # ALTER returns immediately; the controller brings the fresh target replica
    # up next to r1 (both serve while the target hydrates), then cuts over and
    # drops r1. The in-flight window may be short, so don't require observing
    # the overlap, but any state we do observe must never include a legacy
    # "-pending" replica.
    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET (SIZE = 'scale=1,workers=2') WITH ( WAIT FOR '5s')
        """,
        port="internal",
        user="mz_system",
    )
    for _ in range(240):
        names = get_replica_names("zdtaltertest")
        assert not any(
            name.endswith("-pending") for name in names
        ), f"controller reconfiguration must not use pending replicas, found {names}"
        if names == ["r2"]:
            break
        time.sleep(0.25)

    wait_for_replica_names(["r2"])
    assert (
        mz.environmentd.sql_query("""
        SELECT size FROM mz_clusters WHERE name='zdtaltertest';
        """) == (["scale=1,workers=2"],)
    ), "Cluster should use new config after alter completes"

    # Validate cancelation around alter cluster..with: the reconfiguration is a
    # durable record the controller converges on independently of the session,
    # so canceling the issuing backend does not abort it. The cluster still
    # cuts over to the target config.
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 ( SIZE = 'scale=1,workers=1');
        """,
        port="internal",
        user="mz_system",
    )
    wait_for_replica_names(["r1"], cluster="cluster1")

    # We need persistent connection that we can later issue a cancel backend to
    conn = mz.environmentd.sql_conn(
        port="internal",
        user="mz_system",
    )
    conn.autocommit = True

    def query_with_conn(
        sql: str, conn: Connection, ignore_pg_exception=False
    ) -> list[list[Any]]:
        """Execute a SQL query against the service and return results."""
        try:
            with conn.cursor() as cursor:
                LOGGER.info(f"> {sql}")
                cursor.execute(sql)
                return cursor.fetchall()
        except DatabaseError:
            if ignore_pg_exception:
                return []
            else:
                raise

    pid = query_with_conn("select pg_backend_pid();", conn)[0][0]
    query_with_conn(
        """
        ALTER CLUSTER cluster1 SET (SIZE = 'scale=1,workers=2') WITH ( WAIT FOR '5s')
        """,
        conn,
        True,
    )
    mz.environmentd.sql(
        f"select pg_cancel_backend({pid});",
        port="internal",
        user="mz_system",
    )

    wait_for_replica_names(["r2"], cluster="cluster1")
    assert (
        mz.environmentd.sql_query("""
        SELECT size FROM mz_clusters WHERE name='cluster1';
        """) == (["scale=1,workers=2"],)
    ), "Cancel must not abort the durable background reconfiguration"

    # Test zero-downtime reconfig wait until ready
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        DROP CLUSTER IF EXISTS zdtaltertest CASCADE;
        """,
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql("""
        CREATE CLUSTER slow_hydration( SIZE = "scale=1,workers=1" );
        SET CLUSTER TO slow_hydration;
        SET DATABASE TO materialize;
        CREATE TABLE test_table (id int);

        -- this view will take a loong time to run/hydrate
        -- we'll use it to validate timeouts
        CREATE VIEW  test_view AS WITH
        a AS (SELECT generate_series(0,10000) AS a),
        b AS (SELECT generate_series(0,1000) AS b)
        SELECT * FROM  a,b,test_table;

        CREATE INDEX test_view_idx ON test_view(id);
        """)

    # The background ALTER returns immediately; the target replica cannot
    # hydrate the slow view within the timeout, so the controller rolls back at
    # the deadline: it drops the target set and clears the record, leaving the
    # realized config untouched.
    mz.environmentd.sql("""
        ALTER CLUSTER slow_hydration SET (SIZE='scale=1,workers=4') WITH (WAIT UNTIL READY (TIMEOUT='1s', ON TIMEOUT ROLLBACK))
        """)

    # The rollback's definitive signal is the `timed-out` audit transition: the
    # controller writes it when it rolls back at the deadline (drops the target
    # set, clears the record). Poll for that transition before reading the settled
    # state back. We cannot key on the replica set alone: the post-rollback set is
    # just [r1], which is indistinguishable from the transient window before the
    # target replica is even created, so a bare replica-name wait could latch the
    # pre-rollback state and race the deadline.
    for _ in range(240):
        if mz.environmentd.sql_query("""
            SELECT 1 FROM mz_catalog.mz_audit_events
            WHERE event_type = 'alter' AND object_type = 'cluster'
              AND details->>'cluster_name' = 'slow_hydration'
              AND details->>'transition' = 'timed-out';
            """):
            break
        time.sleep(0.25)
    wait_for_replica_names(["r1"], cluster="slow_hydration")
    reconfiguration = mz.environmentd.sql_query("""
        SELECT recon.current_size, recon.target_size, recon.reconfiguration_in_flight
        FROM mz_internal.mz_cluster_reconfigurations recon
        JOIN mz_clusters c ON c.id = recon.cluster_id
        WHERE c.name = 'slow_hydration';
        """)
    assert reconfiguration == (
        ["scale=1,workers=1", "scale=1,workers=1", False],
    ), f"Expected the rolled-back reconfiguration to settle at the realized config, found {reconfiguration}"

    # The timeout's papertrail is the audit log: a started and a timed-out
    # transition, the latter carrying the abandoned target.
    transitions = mz.environmentd.sql_query("""
        SELECT details->>'transition', details->>'target_size'
        FROM mz_catalog.mz_audit_events
        WHERE event_type = 'alter'
          AND object_type = 'cluster'
          AND details->>'cluster_name' = 'slow_hydration'
          AND details->>'transition' IS NOT NULL
        ORDER BY id;
        """)
    assert transitions == (
        ["started", "scale=1,workers=4"],
        ["timed-out", "scale=1,workers=4"],
    ), f"Expected a started and a timed-out transition, found {transitions}"

    # Test fails to alter with source
    mz.environmentd.sql("""
        CREATE CLUSTER cluster_with_source( SIZE = "scale=1,workers=1" );
        SET CLUSTER TO cluster_with_source;
        SET DATABASE TO materialize;
        CREATE SOURCE counter
          FROM LOAD GENERATOR COUNTER
          (TICK INTERVAL '500ms');
        """)

    mz.testdrive.run(
        input=dedent("""
            ! ALTER CLUSTER cluster_with_source set (replication factor 2000) WITH (WAIT UNTIL READY (TIMEOUT='10s', ON TIMEOUT ROLLBACK))
            contains: creating cluster replica would violate max_replicas_per_cluster limit
            """),
        no_reset=True,
    )
