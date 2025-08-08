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
from threading import Thread
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

    mz.environmentd.sql("ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('1', '2', '3'))")
    check = mz.environmentd.sql_query(
        "SELECT list_length(availability_zones) = 3 FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert check is not None
    assert check == True

    mz.testdrive.run(
        input=dedent(
            """
            ! ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('4'))
            exact:unknown cluster replica availability zone 4
            """
        ),
        no_reset=True,
    )

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert [replica[0] for replica in replicas] == ["r1", "r2"]

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("ALTER CLUSTER sized1 SET (REPLICATION FACTOR 1)")

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert [replica[0] for replica in replicas] == ["r1"]

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")

    mz.testdrive.run(
        input=dedent(
            """
            ! CREATE CLUSTER sizedbad (SIZE="badsize")
            contains:unknown cluster replica size badsize
            """
        ),
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
    mz.environmentd.sql(
        """
        ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true;
        ALTER SYSTEM SET enable_multi_replica_sources = true;
        """,
        port="internal",
        user="mz_system",
    )

    def assert_replica_names(names, allow_pending=False):
        replicas = mz.environmentd.sql_query(
            """
            SELECT mz_cluster_replicas.name
            FROM mz_cluster_replicas, mz_clusters
            WHERE mz_cluster_replicas.cluster_id = mz_clusters.id
            AND mz_clusters.name = 'zdtaltertest';
            """
        )
        assert [replica[0] for replica in replicas] == names
        if not allow_pending:
            assert (
                len(
                    mz.environmentd.sql_query(
                        """
                        SELECT cr.name
                        FROM mz_internal.mz_pending_cluster_replicas  ur
                        INNER join mz_cluster_replicas cr ON cr.id=ur.id
                        INNER join mz_clusters c ON c.id=cr.cluster_id
                        WHERE c.name = 'zdtaltertest';
                        """
                    )
                )
                == 0
            ), "There should be no pending replicas"

    # Basic zero-downtime reconfig test cases matrix
    # - size change, no replica change
    # - replica size up, no other change
    # - replica size down, with size change
    # - replica size down, no other change
    # - replica size up, with size change
    # Other assertions
    # - no pending replicas after alter finishes
    # - names should match r# patter, not end with `-pending`
    # - cancelled statements correctly roll back
    # - timedout until ready queries take the appropriate action
    # - Fails to zero-downtime alter cluster with source
    mz.environmentd.sql(
        'CREATE CLUSTER zdtaltertest ( SIZE = "scale=1,workers=1" )',
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=2' ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1", "r2"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=2', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1", "r2"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER zdtaltertest SET ( SIZE = 'scale=1,workers=1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    # Setup for validating cancelation and
    # replica checks during alter
    mz.testdrive.run(
        no_reset=True,
        input=dedent(
            """
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
        """
        ),
    )

    # Valudate replicas are correct during an ongoing alter
    def zero_downtime_alter():
        mz.environmentd.sql(
            """
            ALTER CLUSTER zdtaltertest SET (SIZE = 'scale=1,workers=2') WITH ( WAIT FOR '5s')
            """,
            port="internal",
            user="mz_system",
        )

    thread = Thread(target=zero_downtime_alter)
    thread.start()
    time.sleep(1)

    assert_replica_names(["r1", "r1-pending"], allow_pending=True)
    assert (
        mz.environmentd.sql_query(
            """
        SELECT size FROM mz_clusters WHERE name='zdtaltertest';
        """
        )
        == (["scale=1,workers=1"],)
    ), "Cluster should use original config during alter"

    thread.join()

    assert_replica_names(["r1"], allow_pending=False)
    assert (
        mz.environmentd.sql_query(
            """
        SELECT size FROM mz_clusters WHERE name='zdtaltertest';
        """
        )
        == (["scale=1,workers=2"],)
    ), "Cluster should use new config after alter completes"

    # Validate cancelation of alter cluster..with
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 ( SIZE = 'scale=1,workers=1');
        """,
        port="internal",
        user="mz_system",
    )

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
    thread = Thread(
        target=query_with_conn,
        args=[
            """
            ALTER CLUSTER cluster1 SET (SIZE = 'scale=1,workers=2') WITH ( WAIT FOR '5s')
            """,
            conn,
            True,
        ],
    )
    thread.start()
    time.sleep(1)
    mz.environmentd.sql(
        f"select pg_cancel_backend({pid});",
        port="internal",
        user="mz_system",
    )
    time.sleep(1)

    assert_replica_names(["r1"], allow_pending=False)
    assert (
        mz.environmentd.sql_query(
            """
        SELECT size FROM mz_clusters WHERE name='cluster1';
        """
        )
        == (["scale=1,workers=1"],)
    ), "Cluster should not have updated if canceled during alter"

    # Test zero-downtime reconfig wait until ready
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        DROP CLUSTER IF EXISTS zdtaltertest CASCADE;
        """,
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql(
        """
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
        """
    )

    mz.testdrive.run(
        input=dedent(
            """
            ! ALTER CLUSTER slow_hydration set (size='4') WITH (WAIT UNTIL READY (TIMEOUT='1s', ON TIMEOUT ROLLBACK))
            contains: canceling statement, provided timeout lapsed
            """
        ),
        no_reset=True,
    )

    # Test fails to alter with source
    mz.environmentd.sql(
        """
        CREATE CLUSTER cluster_with_source( SIZE = "scale=1,workers=1" );
        SET CLUSTER TO cluster_with_source;
        SET DATABASE TO materialize;
        CREATE SOURCE counter
          FROM LOAD GENERATOR COUNTER
          (TICK INTERVAL '500ms');
        """
    )

    mz.testdrive.run(
        input=dedent(
            """
            ! ALTER CLUSTER cluster_with_source set (replication factor 2000) WITH (WAIT UNTIL READY (TIMEOUT='10s', ON TIMEOUT ROLLBACK))
            contains: creating cluster replica would violate max_replicas_per_cluster limit
            """
        ),
        no_reset=True,
    )
