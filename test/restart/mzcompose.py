# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Testdrive-based tests involving restarting materialized (including its clusterd
processes). See cluster tests for separate clusterds, see platform-checks for
further restart scenarios.
"""

import json
import time
from textwrap import dedent

import requests
from psycopg.errors import (
    InternalError_,
    OperationalError,
)

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import CockroachOrPostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.ui import UIError

testdrive_no_reset = Testdrive(
    name="testdrive_no_reset",
    no_reset=True,
    materialize_params={"transaction_isolation": "'strict serializable'"},
)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Mz(app_password=""),
    Materialized(),
    Testdrive(
        entrypoint_extra=[
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
        ],
    ),
    testdrive_no_reset,
    CockroachOrPostgresMetadata(),
]


def workflow_retain_history(c: Composition) -> None:
    def check_retain_history(name: str):
        start = time.time()
        while True:
            ts = c.sql_query(
                f"EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM retain_{name}"
            )
            ts = ts[0][0]
            ts = json.loads(ts)
            source = ts["sources"][0]
            since = source["read_frontier"][0]
            upper = source["write_frontier"][0]
            if upper - since > 2000:
                break
            end = time.time()
            # seconds since start
            elapsed = end - start
            if elapsed > 10:
                raise UIError("timeout hit while waiting for retain history")
            time.sleep(0.5)

    def check_retain_history_for(names: list[str]):
        for name in names:
            check_retain_history(name)

    c.up("materialized")
    c.sql(
        "ALTER SYSTEM SET enable_logical_compaction_window = true",
        port=6877,
        user="mz_system",
    )
    c.sql("CREATE TABLE retain_t (i INT)")
    c.sql("INSERT INTO retain_t VALUES (1)")
    c.sql(
        "CREATE MATERIALIZED VIEW retain_mv WITH (RETAIN HISTORY = FOR '2s') AS SELECT * FROM retain_t"
    )
    c.sql(
        "CREATE SOURCE retain_s FROM LOAD GENERATOR COUNTER WITH (RETAIN HISTORY = FOR '5s')"
    )
    names = ["mv", "s"]
    check_retain_history_for(names)

    # Ensure that RETAIN HISTORY is respected on boot.
    c.kill("materialized")
    c.up("materialized")
    check_retain_history_for(names)

    c.kill("materialized")


def workflow_github_2454(c: Composition) -> None:
    c.up("materialized")
    c.run_testdrive_files("github-2454.td")

    # Ensure MZ can boot
    c.kill("materialized")
    c.up("materialized")
    c.kill("materialized")


# Test that `mz_internal.mz_object_dependencies` re-populates.
def workflow_github_5108(c: Composition) -> None:
    c.up("materialized", {"name": "testdrive_no_reset", "persistent": True})

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > CREATE SOURCE with_subsources FROM LOAD GENERATOR AUCTION;
            > CREATE TABLE accounts FROM SOURCE with_subsources (REFERENCE accounts);
            > CREATE TABLE auctions FROM SOURCE with_subsources (REFERENCE auctions);
            > CREATE TABLE bids FROM SOURCE with_subsources (REFERENCE bids);
            > CREATE TABLE organizations FROM SOURCE with_subsources (REFERENCE organizations);
            > CREATE TABLE users FROM SOURCE with_subsources (REFERENCE users);

            > SELECT DISTINCT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id OR s.id = d.object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id OR top_level_s.id = d.referenced_object_id
              WHERE top_level_s.name = 'with_subsources' AND (s.type = 'progress' OR s.type = 'subsource');
            source          subsource
            -------------------------
            with_subsources with_subsources_progress

            > SELECT DISTINCT
              s.name AS source,
              t.name AS table
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id
              JOIN mz_tables AS t ON t.id = d.object_id
              WHERE s.name = 'with_subsources';
            source            table
            -------------------------
            with_subsources   bids
            with_subsources   users
            with_subsources   accounts
            with_subsources   auctions
            with_subsources   organizations
            """
        ),
    )

    # Restart mz
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id OR s.id = d.object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id OR top_level_s.id = d.referenced_object_id
              WHERE top_level_s.name = 'with_subsources' AND (s.type = 'progress' OR s.type = 'subsource');
            source          subsource
            -------------------------
            with_subsources with_subsources_progress

            > SELECT DISTINCT
              s.name AS source,
              t.name AS table
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id
              JOIN mz_tables AS t ON t.id = d.object_id
              WHERE s.name = 'with_subsources';
            source            table
            -------------------------
            with_subsources   bids
            with_subsources   users
            with_subsources   accounts
            with_subsources   auctions
            with_subsources   organizations

            """
        ),
    )

    c.kill("materialized")


def workflow_audit_log(c: Composition) -> None:
    c.up("materialized")

    # Create some audit log entries.
    c.sql("CREATE TABLE t (i INT)")
    c.sql("CREATE DEFAULT INDEX ON t")

    log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify the audit log entries are still present and have not changed.
    restart_log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")
    if log != restart_log or not log:
        print("initial audit log:", log)
        print("audit log after restart:", restart_log)
        raise Exception("audit logs emtpy or not equal after restart")


def workflow_stash(c: Composition) -> None:
    c.rm(
        "testdrive",
        "materialized",
        stop=True,
        destroy_volumes=True,
    )
    c.rm_volumes("mzdata", force=True)

    with c.override(Materialized(external_metadata_store=True)):
        c.up(c.metadata_store())

        c.up("materialized")

        cursor = c.sql_cursor()
        cursor.execute("CREATE TABLE a (i INT)")

        c.stop(c.metadata_store())
        c.up(c.metadata_store())

        cursor.execute("CREATE TABLE b (i INT)")

        # No implicit restart as sanity check here, will panic:
        # https://github.com/MaterializeInc/database-issues/issues/6168
        c.down(sanity_restart_mz=False)


def workflow_storage_managed_collections(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized")

    # Create some storage shard entries.
    c.sql("CREATE TABLE t (i INT)")

    # Storage collections are eventually consistent, so loop to be sure updates
    # have made it.

    user_shards: list[str] = []
    while len(user_shards) == 0:
        user_shards = c.sql_query(
            "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%';"
        )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify the shard mappings are still present and have not changed.
    restart_user_shards: list[str] = []
    while len(restart_user_shards) == 0:
        restart_user_shards = c.sql_query(
            "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%';"
        )

    if user_shards != restart_user_shards or not user_shards:
        print("initial user shards:", user_shards)
        print("user shards after restart:", restart_user_shards)
        raise Exception("user shards empty or not equal after restart")


def workflow_allowed_cluster_replica_sizes(c: Composition) -> None:
    c.up("materialized", {"name": "testdrive_no_reset", "persistent": True})

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # We can create a cluster with sizes '1' and '2'
            > CREATE CLUSTER test REPLICAS (r1 (SIZE '1'), r2 (SIZE '2'))

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true ""
            test r2 2 true ""

            # We cannot create replicas with size '2' after restricting allowed_cluster_replica_sizes to '1'
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = '1'

            ! CREATE CLUSTER REPLICA test.r3 SIZE '2'
            contains:unknown cluster replica size 2
            """
        ),
    )

    # Assert that mz restarts successfully even in the presence of replica sizes that are not allowed
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Cluster replica of disallowed sizes still exist
            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true ""
            test r2 2 true ""

            # We cannot create replicas with size '2' (system parameter value persists across restarts)
            ! CREATE CLUSTER REPLICA test.r3 SIZE '2'
            contains:unknown cluster replica size 2

            # We can create replicas with size '2' after listing that size as allowed
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = '1', '2'

            > CREATE CLUSTER REPLICA test.r3 SIZE '2'

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true ""
            test r2 2 true ""
            test r3 2 true ""
            """
        ),
    )

    # Assert that the persisted allowed_cluster_replica_sizes (a setting that
    # supports multiple values) is correctly restored on restart.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SHOW allowed_cluster_replica_sizes
            "\\"1\\", \\"2\\""

            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Reset for following tests
            $ postgres-execute connection=mz_system
            ALTER SYSTEM RESET allowed_cluster_replica_sizes
            """
        ),
    )


def workflow_allow_user_sessions(c: Composition) -> None:
    c.up("materialized")
    http_port = c.port("materialized", 6876)

    # Ensure new user sessions are allowed.
    c.sql(
        "ALTER SYSTEM SET allow_user_sessions = true",
        port=6877,
        user="mz_system",
    )

    # SQL and HTTP user sessions should work.
    assert c.sql_query("SELECT 1") == [(1,)]
    assert requests.post(
        f"http://localhost:{http_port}/api/sql", json={"query": "select 1"}
    ).json()["results"][0]["rows"] == [["1"]]

    # Save a cursor for later.
    cursor = c.sql_cursor()

    # Disallow new user sessions.
    c.sql(
        "ALTER SYSTEM SET allow_user_sessions = false",
        port=6877,
        user="mz_system",
    )

    # New SQL and HTTP user sessions should now fail.
    try:
        c.sql_query("SELECT 1")
    except OperationalError as e:
        # assert e.pgcode == "MZ010" # Not exposed by psycopg
        assert "login blocked" in str(e)
        assert (
            "DETAIL:  Your organization has been blocked. Please contact support."
            in e.args[0]
        ), e.args

    res = requests.post(
        f"http://localhost:{http_port}/api/sql", json={"query": "select 1"}
    )
    assert res.status_code == 403
    assert res.json() == {
        "message": "login blocked",
        "code": "MZ010",
        "detail": "Your organization has been blocked. Please contact support.",
    }

    # The cursor from the beginning of the test should still work.
    cursor.execute("SELECT 1")
    assert cursor.fetchall() == [(1,)]

    # Re-allow new user sessions.
    c.sql(
        "ALTER SYSTEM SET allow_user_sessions = true",
        port=6877,
        user="mz_system",
    )

    # SQL and HTTP user sessions should work again.
    assert c.sql_query("SELECT 1") == [(1,)]
    assert requests.post(
        f"http://localhost:{http_port}/api/sql", json={"query": "select 1"}
    ).json()["results"][0]["rows"] == [["1"]]

    # The cursor from the beginning of the test should still work.
    cursor.execute("SELECT 1")
    assert cursor.fetchall() == [(1,)]


def workflow_network_policies(c: Composition) -> None:
    c.up("materialized")
    http_port = c.port("materialized", 6876)

    # ensure default network policy
    def assert_can_connect():
        assert c.sql_query("SELECT 1") == [(1,)]
        assert requests.post(
            f"http://localhost:{http_port}/api/sql", json={"query": "select 1"}
        ).json()["results"][0]["rows"] == [["1"]]

    def assert_new_connection_fails():
        # New SQL and HTTP user sessions should now fail.
        try:
            c.sql_query("SELECT 1")
        except OperationalError as e:
            # assert e.pgcode == "MZ010" # Not exposed by psycopg
            assert "session denied" in str(e)
            assert "DETAIL:  Access denied for address" in e.args[0], e.args

        res = requests.post(
            f"http://localhost:{http_port}/api/sql", json={"query": "select 1"}
        )
        assert res.status_code == 403
        assert res.json()["message"] == "session denied"
        assert res.json()["code"] == "MZ011"
        assert "Access denied for address" in res.json()["detail"]

    # ensure default network policy
    assert c.sql_query("show network_policy") == [("default",)]
    assert_can_connect()

    # enable network policy management
    c.sql(
        "ALTER SYSTEM SET enable_network_policies = true",
        port=6877,
        user="mz_system",
    )

    # assert we can't change the network policy to one that doesn't exist.
    try:
        c.sql_query(
            "ALTER SYSTEM SET network_policy='apples'",
            port=6877,
            user="mz_system",
        )
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "no network policy with such name exists" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError(
            "ALTER SYSTEM SET network_policy didn't return the expected error"
        )

    # close network policies
    c.sql(
        "CREATE NETWORK POLICY closed (RULES ())",
        port=6877,
        user="mz_system",
    )
    c.sql(
        "ALTER SYSTEM SET network_policy='closed'",
        port=6877,
        user="mz_system",
    )
    assert_new_connection_fails()

    # can't drop the actively set network policy.
    try:
        c.sql_query(
            "DROP NETWORK POLICY closed",
            port=6877,
            user="mz_system",
        )
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "network policy is currently in use" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError("DROP NETWORK POLICY didn't return the expected error")

    # open the closed network policy
    c.sql(
        "ALTER NETWORK POLICY closed SET (RULES (open (ACTION='allow', DIRECTION='ingress', ADDRESS='0.0.0.0/0')))",
        port=6877,
        user="mz_system",
    )
    assert_can_connect()
    cursor = c.sql_cursor()

    # shut down the closed network policy
    c.sql(
        "ALTER NETWORK POLICY closed SET (RULES (closed (ACTION='allow', DIRECTION='ingress', ADDRESS='0.0.0.0/32')))",
        port=6877,
        user="mz_system",
    )
    assert_new_connection_fails()

    # validate that the cursor from the beginning of the test still works.
    assert cursor.execute("SELECT 1").fetchall() == [(1,)]

    c.sql(
        "ALTER SYSTEM SET network_policy='default'",
        port=6877,
        user="mz_system",
    )
    c.sql(
        "DROP NETWORK POLICY closed",
        port=6877,
        user="mz_system",
    )


def workflow_drop_materialize_database(c: Composition) -> None:
    c.up("materialized")

    # Drop materialize database
    c.sql(
        "DROP DATABASE materialize",
        port=6877,
        user="mz_system",
    )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify that materialize hasn't blown up
    c.sql("SELECT 1")

    # Restore for next tests
    c.sql(
        "CREATE DATABASE materialize",
        port=6877,
        user="mz_system",
    )
    c.sql(
        "GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO materialize",
        port=6877,
        user="mz_system",
    )


def workflow_bound_size_mz_status_history(c: Composition) -> None:
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "materialized",
        {"name": "testdrive_no_reset", "persistent": True},
    )

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ kafka-create-topic topic=status-history

            > CREATE CONNECTION kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${testdrive.schema-registry-url}'
              );

            > CREATE SOURCE kafka_source
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')

            > CREATE TABLE kafka_source_tbl FROM SOURCE kafka_source (REFERENCE "testdrive-status-history-${testdrive.seed}")
              FORMAT TEXT

            > CREATE SINK kafka_sink
              FROM kafka_source_tbl
              INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM

            $ kafka-verify-topic sink=materialize.public.kafka_sink
            """
        ),
    )

    # Fill mz_source_status_history and mz_sink_status_history up with enough events
    for i in range(5):
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(
                """
                > ALTER CONNECTION kafka_conn SET (BROKER 'dne') WITH (VALIDATE = false);
                > ALTER CONNECTION kafka_conn SET (BROKER '${testdrive.kafka-addr}') WITH (VALIDATE = true);
                """
            ),
        )

    # Verify that we have enough events so that they can be truncated
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT COUNT(*) > 7 FROM mz_internal.mz_source_status_history
            true

            > SELECT COUNT(*) > 7 FROM mz_internal.mz_sink_status_history
            true
            """
        ),
    )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify that we have fewer events now
    # 14 resp. because the truncation default is 5, and the restarted
    # objects produce a new starting and running event.
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT COUNT(*) FROM mz_internal.mz_source_status_history
            14

            > SELECT COUNT(*) FROM mz_internal.mz_sink_status_history
            7
            """
        ),
    )


def workflow_bound_size_mz_cluster_replica_metrics_history(c: Composition) -> None:
    """
    Test the truncation mechanism for `mz_cluster_replica_metrics_history`.
    """

    c.down(destroy_volumes=True)
    c.up("materialized", {"name": "testdrive_no_reset", "persistent": True})

    # The replica metrics are updated once per minute and on envd startup. We
    # can thus restart envd to generate metrics rows without having to block
    # for a minute.

    # Create a replica and wait for metrics data to arrive.
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > CREATE CLUSTER test SIZE '1'

            > SELECT count(*) >= 1
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """
        ),
    )

    # The default retention interval is 30 days, so we don't expect truncation
    # after a restart.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT count(*) >= 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """
        ),
    )

    # Reduce the retention interval to force a truncation.
    c.sql(
        "ALTER SYSTEM SET replica_metrics_history_retention_interval = '1s'",
        port=6877,
        user="mz_system",
    )

    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT count(*) < 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """
        ),
    )

    # Verify that this also works a second time.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT count(*) < 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """
        ),
    )


def workflow_index_compute_dependencies(c: Composition) -> None:
    """
    Assert that materialized views and index catalog items see and use only
    indexes created before them upon restart.

    Various parts of the optimizer internals and tooling, such as

    - `EXPLAIN REPLAN`
    - `bin/mzcompose clone defs`

    are currently depending on the fact that the `GlobalId` ordering respects
    dependency ordering. In other words, if an index `i` is created after a
    catalog item `x`, then `x` cannot use `i` even after restart.

    This test should codify this assumption so we can get an early signal if
    this is broken for some reason in the future.
    """
    c.up("materialized", {"name": "testdrive_no_reset", "persistent": True})

    def depends_on(c: Composition, obj_name: str, dep_name: str, expected: bool):
        """Check whether `(obj_name, dep_name)` is a compute dependency or not."""
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(
                f"""
                > (
                    SELECT
                      true
                    FROM
                      mz_catalog.mz_objects as obj
                    WHERE
                      obj.name = '{obj_name}' AND
                      obj.id IN (
                        SELECT
                          cd.object_id
                        FROM
                          mz_internal.mz_compute_dependencies cd JOIN
                          mz_objects dep ON (cd.dependency_id = dep.id)
                        WHERE
                          dep.name = '{dep_name}'
                      )
                  ) UNION (
                    SELECT
                      false
                    FROM
                      mz_catalog.mz_objects as obj
                    WHERE
                      obj.name = '{obj_name}' AND
                      obj.id NOT IN (
                        SELECT
                          cd.object_id
                        FROM
                          mz_internal.mz_compute_dependencies cd JOIN
                          mz_objects dep ON (cd.dependency_id = dep.id)
                        WHERE
                          dep.name = '{dep_name}'
                      )
                  );
                {str(expected).lower()}
                """
            ),
        )

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > DROP TABLE IF EXISTS t1 CASCADE;
            > DROP TABLE IF EXISTS t2 CASCADE;

            > CREATE TABLE t1(x int, y int);
            > CREATE TABLE t2(y int, z int);

            > CREATE INDEX ON t1(y);

            > CREATE VIEW v1 AS SELECT * FROM t1 JOIN t2 USING (y);
            > CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM v1;
            > CREATE INDEX ix1 ON v1(x);

            > CREATE INDEX ON t2(y);

            > CREATE VIEW v2 AS SELECT * FROM t2 JOIN t1 USING (y);
            > CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM v2;
            > CREATE INDEX ix2 ON v2(x);
            """
        ),
    )

    # Verify that mv1 and ix1 depend on t1_y_idx but not on t2_y_idx.
    depends_on(c, "mv1", "t1_y_idx", True)
    depends_on(c, "mv1", "t2_y_idx", False)
    depends_on(c, "ix1", "t1_y_idx", True)
    depends_on(c, "ix1", "t2_y_idx", False)
    # Verify that mv2 and ix2 depend on both t1_y_idx and t2_y_idx.
    depends_on(c, "mv2", "t1_y_idx", True)
    depends_on(c, "mv2", "t2_y_idx", True)
    depends_on(c, "ix2", "t1_y_idx", True)
    depends_on(c, "ix2", "t2_y_idx", True)

    # Restart mz. We expect the index on t2(y) to not be visible to ix1 and mv1
    # after the restart as well.
    c.kill("materialized")
    c.up("materialized")

    # Verify that mv1 and ix1 depend on t1_y_idx but not on t2_y_idx.
    depends_on(c, "mv1", "t1_y_idx", True)
    depends_on(c, "mv1", "t2_y_idx", False)
    depends_on(c, "ix1", "t1_y_idx", True)
    depends_on(c, "ix1", "t2_y_idx", False)
    # Verify that mv2 and ix2 depend on both t1_y_idx and t2_y_idx.
    depends_on(c, "mv2", "t1_y_idx", True)
    depends_on(c, "mv2", "t2_y_idx", True)
    depends_on(c, "ix2", "t1_y_idx", True)
    depends_on(c, "ix2", "t2_y_idx", True)


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    files = buildkite.shard_list(list(c.workflows.keys()), lambda workflow: workflow)
    c.test_parts(files, process)
