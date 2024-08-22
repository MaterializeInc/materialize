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

    mz.environmentd.sql(f"CREATE CLUSTER sized1 SIZE '{SIZE}-1', REPLICATION FACTOR 2")
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
        'ALTER SYSTEM SET ALLOWED_CLUSTER_REPLICA_SIZES="1"',
        port="internal",
        user="mz_system",
    )
    try:
        mz.environmentd.sql(
            'CREATE CLUSTER mzsizetest (SIZE="2")',
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


def test_graceful_reconfiguration(mz: MaterializeApplication) -> None:
    mz.environmentd.sql(
        """
        ALTER SYSTEM SET enable_graceful_cluster_reconfiguration = true;
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
            AND mz_clusters.name = 'gracefulatlertest';
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
                        WHERE c.name = 'gracefulatlertest';
                        """
                    )
                )
                == 0
            ), "There should be no pending replicas"

    # Basic Graceful reocnfig test cases matrix
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
    mz.environmentd.sql(
        'CREATE CLUSTER gracefulatlertest ( SIZE = "1" )',
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql(
        """
        ALTER CLUSTER gracefulatlertest SET ( SIZE = '2' ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER gracefulatlertest SET ( SIZE = '1', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1", "r2"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER gracefulatlertest SET ( SIZE = '1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER gracefulatlertest SET ( SIZE = '2', REPLICATION FACTOR 2 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1", "r2"])

    mz.environmentd.sql(
        """
        ALTER CLUSTER gracefulatlertest SET ( SIZE = '1', REPLICATION FACTOR 1 ) WITH ( WAIT FOR '1ms' )
        """,
        port="internal",
        user="mz_system",
    )
    assert_replica_names(["r1"])

    # Setup for validating cancelation and
    # replica checks during alter
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS gracefulatlertest CASCADE;
        DROP TABLE IF EXISTS t CASCADE;

        CREATE CLUSTER gracefulatlertest ( SIZE = '1');

        SET CLUSTER = gracefulatlertest;

        -- now let's give it another go with user-defined objects
        CREATE TABLE t (a int);
        CREATE DEFAULT INDEX ON t;
        INSERT INTO t VALUES (42);
        GRANT ALL ON CLUSTER gracefulatlertest TO materialize;
        """,
        port="internal",
        user="mz_system",
    )

    # Valudate replicas are correct during an ongoing alter
    def gracefully_alter():
        mz.environmentd.sql(
            """
            ALTER CLUSTER gracefulatlertest SET (SIZE = '2') WITH ( WAIT FOR '5s')
            """,
            port="internal",
            user="mz_system",
        )

    thread = Thread(target=gracefully_alter)
    thread.start()
    time.sleep(1)

    assert_replica_names(["r1", "r1-pending"], allow_pending=True)
    assert (
        mz.environmentd.sql_query(
            """
        SELECT size FROM mz_clusters WHERE name='gracefulatlertest';
        """
        )
        == (["1"],)
    ), "Cluster should use original config during alter"

    thread.join()

    assert_replica_names(["r1"], allow_pending=False)
    assert (
        mz.environmentd.sql_query(
            """
        SELECT size FROM mz_clusters WHERE name='gracefulatlertest';
        """
        )
        == (["2"],)
    ), "Cluster should use new config after alter completes"

    # Validate cancelation of alter cluster..with
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 ( SIZE = '1');
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
            ALTER CLUSTER cluster1 SET (SIZE = '2') WITH ( WAIT FOR '5s')
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
        == (["1"],)
    ), "Cluster should not have updated if canceled during alter"

    # Test graceful reconfig wait until ready
    mz.environmentd.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        DROP CLUSTER IF EXISTS gracefulaltertest CASCADE;
        """,
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql(
        """
        CREATE CLUSTER slow_hydration( SIZE = "1" );
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
