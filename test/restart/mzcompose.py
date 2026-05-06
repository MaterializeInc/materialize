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

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.ui import UIError

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Mz(app_password=""),
    Materialized(),
    Testdrive(
        entrypoint_extra=[
            f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
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
    c.sql("CREATE SOURCE retain_s FROM LOAD GENERATOR COUNTER")
    c.sql(
        "CREATE TABLE retain_s_tbl FROM SOURCE retain_s WITH (RETAIN HISTORY = FOR '5s')"
    )
    names = ["mv", "s_tbl"]
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
    c.up("materialized", Service("testdrive_no_reset", idle=True))

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
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
            """),
    )

    # Restart mz
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SELECT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id OR s.id = d.object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id OR top_level_s.id = d.referenced_object_id
              WHERE top_level_s.name = 'with_subsources' AND (s.type = 'progress' OR s.type = 'subsource');
            source          subsource
            -------------------------

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

            """),
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
    c.up("materialized", Service("testdrive_no_reset", idle=True))

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # We can create a cluster with sizes 'scale=1,workers=1' and 'scale=1,workers=2'
            > CREATE CLUSTER test REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=2'))

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 scale=1,workers=1 true ""
            test r2 scale=1,workers=2 true ""

            # We cannot create replicas with size 'scale=1,workers=2' after restricting allowed_cluster_replica_sizes to 'scale=1,workers=1'
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = 'scale=1,workers=1'

            ! CREATE CLUSTER REPLICA test.r3 SIZE 'scale=1,workers=2'
            contains:unknown cluster replica size scale=1,workers=2
            """),
    )

    # Assert that mz restarts successfully even in the presence of replica sizes that are not allowed
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Cluster replica of disallowed sizes still exist
            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 scale=1,workers=1 true ""
            test r2 scale=1,workers=2 true ""

            # We cannot create replicas with size 'scale=1,workers=2' (system parameter value persists across restarts)
            ! CREATE CLUSTER REPLICA test.r3 SIZE 'scale=1,workers=2'
            contains:unknown cluster replica size scale=1,workers=2

            # We can create replicas with size 'scale=1,workers=2' after listing that size as allowed
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = 'scale=1,workers=1', 'scale=1,workers=2'

            > CREATE CLUSTER REPLICA test.r3 SIZE 'scale=1,workers=2'

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 scale=1,workers=1 true ""
            test r2 scale=1,workers=2 true ""
            test r3 scale=1,workers=2 true ""
            """),
    )

    # Assert that the persisted allowed_cluster_replica_sizes (a setting that
    # supports multiple values) is correctly restored on restart.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SHOW allowed_cluster_replica_sizes
            "\\"scale=1,workers=1\\", \\"scale=1,workers=2\\""

            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Reset for following tests
            $ postgres-execute connection=mz_system
            ALTER SYSTEM RESET allowed_cluster_replica_sizes
            """),
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


def workflow_mcp_feature_flags(c: Composition) -> None:
    """Test that enable_mcp_agent and enable_mcp_developer dyncfg flags
    can disable MCP endpoints without a restart."""

    mcp_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1.0"},
        },
    }

    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth.json",
        )
    ):
        c.up("materialized")
        http_port = c.port("materialized", 6876)

        agent_url = f"http://localhost:{http_port}/api/mcp/agent"
        developer_url = f"http://localhost:{http_port}/api/mcp/developer"

        # Both endpoints should be enabled by default.
        assert requests.post(agent_url, json=mcp_request).status_code == 200
        assert requests.post(developer_url, json=mcp_request).status_code == 200

        # Disable the agent endpoint individually.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent = false",
            port=6877,
            user="mz_system",
        )

        assert requests.post(agent_url, json=mcp_request).status_code == 503
        # Developer should still be enabled.
        assert requests.post(developer_url, json=mcp_request).status_code == 200

        # Disable developer too.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_developer = false",
            port=6877,
            user="mz_system",
        )

        assert requests.post(developer_url, json=mcp_request).status_code == 503

        # Re-enable agent — developer should remain disabled.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent = true",
            port=6877,
            user="mz_system",
        )

        assert requests.post(agent_url, json=mcp_request).status_code == 200
        assert requests.post(developer_url, json=mcp_request).status_code == 503

        # Re-enable developer.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_developer = true",
            port=6877,
            user="mz_system",
        )

        assert requests.post(agent_url, json=mcp_request).status_code == 200
        assert requests.post(developer_url, json=mcp_request).status_code == 200

        c.kill("materialized")


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
        Service("testdrive_no_reset", idle=True),
    )

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
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
            """),
    )

    # Fill mz_source_status_history and mz_sink_status_history up with enough events
    for i in range(5):
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent("""
                > ALTER CONNECTION kafka_conn SET (BROKER 'dne') WITH (VALIDATE = false);
                > ALTER CONNECTION kafka_conn SET (BROKER '${testdrive.kafka-addr}') WITH (VALIDATE = true);
                """),
        )

    # Verify that we have enough events so that they can be truncated
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SELECT COUNT(*) > 7 FROM mz_internal.mz_source_status_history
            true

            > SELECT COUNT(*) > 7 FROM mz_internal.mz_sink_status_history
            true
            """),
    )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify that we have fewer events now
    # 14 resp. because the truncation default is 5, and the restarted
    # objects produce a new starting and running event.
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SELECT COUNT(*) FROM mz_internal.mz_source_status_history
            14

            > SELECT COUNT(*) FROM mz_internal.mz_sink_status_history
            7
            """),
    )


def workflow_bound_size_mz_cluster_replica_metrics_history(c: Composition) -> None:
    """
    Test the truncation mechanism for `mz_cluster_replica_metrics_history`.
    """

    c.down(destroy_volumes=True)
    c.up("materialized", Service("testdrive_no_reset", idle=True))

    # The replica metrics are updated once per minute and on envd startup. We
    # can thus restart envd to generate metrics rows without having to block
    # for a minute.

    # Create a replica and wait for metrics data to arrive.
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > CREATE CLUSTER test SIZE 'scale=1,workers=1'

            > SELECT count(*) >= 1
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """),
    )

    # The default retention interval is 30 days, so we don't expect truncation
    # after a restart.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SELECT count(*) >= 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """),
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
        input=dedent("""
            > SELECT count(*) < 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """),
    )

    # Verify that this also works a second time.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
            > SELECT count(*) < 2
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas r ON r.id = m.replica_id
              JOIN mz_clusters c ON c.id = r.cluster_id
              WHERE c.name = 'test'
            true
            """),
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
    c.up("materialized", Service("testdrive_no_reset", idle=True))

    def depends_on(c: Composition, obj_name: str, dep_name: str, expected: bool):
        """Check whether `(obj_name, dep_name)` is a compute dependency or not."""
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(f"""
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
                """),
        )

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent("""
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
            """),
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


def workflow_user_id_no_reuse_after_restart(c: Composition) -> None:
    """Verify that batch-allocated user IDs are never reused across restarts.

    Uses a small batch size so unused IDs in the pool are discarded on
    shutdown. After restart a fresh batch is allocated, so all new IDs
    must be strictly greater than every pre-restart ID.
    """

    def user_id_nums(c: Composition, name_prefix: str) -> list[int]:
        """Return the numeric part of user IDs for objects matching the prefix."""
        rows = c.sql_query(
            f"SELECT id FROM mz_objects WHERE name LIKE '{name_prefix}%'"
        )
        # IDs look like 'u123'; extract the numeric suffix.
        return sorted(int(row[0].lstrip("u")) for row in rows)

    c.down(destroy_volumes=True)
    c.up("materialized")

    # Set a small batch size so most of the pool is unused at restart.
    c.sql(
        "ALTER SYSTEM SET user_id_pool_batch_size = 5",
        port=6877,
        user="mz_system",
    )

    # --- Phase 1: create objects before restart ---
    c.sql("CREATE TABLE idreuse_t1 (a INT)")
    c.sql("CREATE TABLE idreuse_t2 (b INT)")
    c.sql("CREATE VIEW idreuse_v1 AS SELECT * FROM idreuse_t1")

    ids_before = user_id_nums(c, "idreuse_")
    assert len(ids_before) == 3, f"expected 3 objects, got {ids_before}"
    max_id_before = max(ids_before)

    # --- Restart ---
    c.kill("materialized")
    c.up("materialized")

    # --- Phase 2: create objects after restart ---
    c.sql("CREATE TABLE idreuse_t3 (c INT)")
    c.sql("CREATE VIEW idreuse_v2 AS SELECT * FROM idreuse_t1")
    c.sql("CREATE MATERIALIZED VIEW idreuse_mv1 AS SELECT count(*) FROM idreuse_t1")

    ids_after = user_id_nums(c, "idreuse_")
    # The 3 pre-restart objects should still exist, plus 3 new ones.
    assert len(ids_after) == 6, f"expected 6 objects, got {ids_after}"

    new_ids = [i for i in ids_after if i not in ids_before]
    assert len(new_ids) == 3, f"expected 3 new IDs, got {new_ids}"

    min_new_id = min(new_ids)
    assert min_new_id > max_id_before, (
        f"ID reuse detected! max pre-restart ID = {max_id_before}, "
        f"but post-restart IDs include {min_new_id}"
    )

    # --- Cleanup ---
    c.sql("DROP MATERIALIZED VIEW idreuse_mv1")
    c.sql("DROP VIEW idreuse_v2")
    c.sql("DROP VIEW idreuse_v1")
    c.sql("DROP TABLE idreuse_t3")
    c.sql("DROP TABLE idreuse_t2")
    c.sql("DROP TABLE idreuse_t1")


def workflow_arrangement_sizes_hydration_gate(c: Composition) -> None:
    """Regression test: every history row tagged `hydration_complete = true`
    must reflect the fully-built arrangement, never a mid-build size.

    After a restart the per-replica introspection-subscribes
    (`mz_compute_hydration_times` and `mz_object_arrangement_sizes`)
    re-install at independent rates, racing the collection timer. The
    race is narrow, so we repeat the kill/up/observe cycle several
    times to keep the test reliable.

    The test workload doesn't change, so the steady-state size of each
    arrangement is constant. Mid-build snapshots are recorded with
    `hydration_complete = false` and may carry any in-flight size — we
    don't assert against them. But every row marked `true` should match
    the steady-state size; a `true` row with a different size means the
    snapshot path mistakenly tagged a mid-build row as complete. The
    history table is append-only, so any such mismatch persists until
    the retention pruner removes it and is visible to a scan after
    settling.
    """

    num_replicas = 2
    arranged_names = tuple(f"idx{i}" for i in range(1, 21))
    expected_count = len(arranged_names) * num_replicas
    name_filter = "(" + ", ".join(f"'{n}'" for n in arranged_names) + ")"
    restart_rounds = 5

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "arrangement_size_history_collection_interval": "500ms",
            },
            sanity_restart=False,
        )
    ):
        c.up("materialized")

        index_ddls = "\n".join(
            f"CREATE INDEX idx{i} IN CLUSTER test ON v ((a + {i}));"
            for i in range(1, 21)
        )
        c.sql(dedent(f"""
                CREATE CLUSTER test SIZE 'scale=1,workers=1', REPLICATION FACTOR {num_replicas};
                CREATE TABLE t (a int, b text);
                INSERT INTO t SELECT g, repeat('x', 1024) FROM generate_series(1, 30000) g;
                CREATE VIEW v AS SELECT a, b FROM t;
                {index_ddls}
                """))

        def wait_for_full_sample() -> None:
            deadline = time.time() + 120
            while time.time() < deadline:
                rows = c.sql_query(f"""
                    SELECT 1 FROM mz_internal.mz_object_arrangement_size_history h
                      JOIN mz_objects o ON o.id = h.object_id
                     WHERE o.name IN {name_filter}
                     GROUP BY h.collection_timestamp
                    HAVING count(*) = {expected_count}
                     LIMIT 1
                    """)
                if rows:
                    return
                time.sleep(0.5)
            raise UIError(f"timed out waiting for a sample with {expected_count} rows")

        for _ in range(restart_rounds):
            wait_for_full_sample()
            c.kill("materialized")
            c.up("materialized")
            # The kill/up just put every replica back into rehydration.
            # Sleep so several 500 ms snapshots fire while the
            # arrangements are still rebuilding — that's when a buggy
            # snapshot would record a partial size.
            time.sleep(8)

        # Require all expected pairs to be present in
        # `mz_object_arrangement_sizes` after settling; otherwise the
        # size check below would trivially pass on an empty dataset.
        wait_for_full_sample()
        steady_state_rows = c.sql_query(f"""
            SELECT s.replica_id, o.name, s.size
              FROM mz_internal.mz_object_arrangement_sizes s
              JOIN mz_objects o ON o.id = s.object_id
             WHERE o.name IN {name_filter}
            """)
        expected_size_for: dict[tuple[str, str], int] = {
            (replica_id, name): size for replica_id, name, size in steady_state_rows
        }
        assert len(expected_size_for) == expected_count, (
            f"expected {expected_count} `(replica_id, object_id)` pairs in "
            f"mz_object_arrangement_sizes after settling; got "
            f"{len(expected_size_for)}: {sorted(expected_size_for)}"
        )

        # Only rows tagged `hydration_complete = true` should match the
        # steady-state size; mid-build rows (`hydration_complete = false`)
        # are recorded but expected to differ.
        history_rows = c.sql_query(f"""
            SELECT h.collection_timestamp::text, h.replica_id, o.name, h.size
              FROM mz_internal.mz_object_arrangement_size_history h
              JOIN mz_objects o ON o.id = h.object_id
             WHERE o.name IN {name_filter}
               AND h.hydration_complete
             ORDER BY h.collection_timestamp, h.replica_id, o.name
            """)
        mismatches: list[tuple[str, str, str, int, int]] = []
        for ts, replica_id, name, size in history_rows:
            expected = expected_size_for[(replica_id, name)]
            if size != expected:
                mismatches.append((ts, replica_id, name, size, expected))
        assert not mismatches, (
            f"{len(mismatches)} history rows tagged `hydration_complete = true` "
            f"recorded a size different from the steady-state size of the same "
            f"`(replica_id, object_id)` pair (would indicate a snapshot wrote a "
            f"mid-build row while marking it complete); first 10: {mismatches[:10]}"
        )


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    files = buildkite.shard_list(list(c.workflows.keys()), lambda workflow: workflow)
    c.test_parts(files, process)
