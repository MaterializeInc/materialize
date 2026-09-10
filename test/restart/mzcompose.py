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

import copy
import json
import math
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from textwrap import dedent

import requests
from psycopg import Error as PsycopgError
from psycopg.errors import (
    InternalError_,
    OperationalError,
)
from urllib3.exceptions import ReadTimeoutError

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose import cluster_replica_size_map
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

SERVICES = [
    Kafka(
        auto_create_topics=True,
        advertised_listeners=[
            "PLAINTEXT://kafka:9092",
            "PLAINTEXT2://kafka:9093",
        ],
        # Move the KRaft controller port off 9093, which the second
        # PLAINTEXT2 listener above uses.
        controller_port=29093,
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT2:PLAINTEXT",
        ],
    ),
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
        start = time.monotonic()
        while True:
            ts = c.sql_query(
                f"EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM retain_{name}"
            )
            ts = ts[0][0]
            ts = json.loads(ts)
            source = ts["sources"][0]
            since = source["read_frontier"][0]
            upper = source["write_frontier"][0]
            # The write frontier is exclusive, so an exact 2,000 ms gap retains
            # the requested two seconds of history.
            if upper - since >= 2000:
                break
            elapsed = time.monotonic() - start
            if elapsed > 10:
                raise UIError(
                    f"timeout hit while waiting for retain history for retain_{name}: "
                    f"read frontier {since}, write frontier {upper}"
                )
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

            # Reset for following tests
            $ postgres-execute connection=mz_system
            ALTER SYSTEM RESET allowed_cluster_replica_sizes
            """),
    )


def workflow_disabled_cluster_replica_size_survives_restart(c: Composition) -> None:
    # SQL-306: disabling a size in `cluster_replica_sizes` that an existing
    # replica still uses must not crash the environment at startup. Disabling
    # is how you retire a size while leaving existing replicas running, so
    # those replicas keep working and only new replicas of that size are
    # refused.
    c.down(destroy_volumes=True)

    sizes = cluster_replica_size_map()
    size = "scale=2,workers=4"
    assert (
        size in sizes and not sizes[size]["disabled"]
    ), f"test assumes {size} exists and is enabled in the default size map"

    # Boot with the size enabled and create a replica that uses it.
    with c.override(Materialized(cluster_replica_size=sizes)):
        c.up("materialized", Service("testdrive_no_reset", idle=True))
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(f"""
                > CREATE CLUSTER test REPLICAS (r1 (SIZE '{size}'))

                > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
                test r1 {size} true ""
                """),
        )
        c.kill("materialized")

    # Restart with that size disabled. Startup rebuilds each replica from its
    # durable size in apply_cluster_replica_update, so a disabled size must not
    # stop the environment from booting and the existing replica must survive.
    # A new replica of the disabled size is still refused.
    disabled = copy.deepcopy(sizes)
    disabled[size]["disabled"] = True
    with c.override(Materialized(cluster_replica_size=disabled)):
        c.up("materialized", Service("testdrive_no_reset", idle=True))
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(f"""
                # Existing replica of the now-disabled size survives the restart.
                > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
                test r1 {size} true ""

                # Creating a new replica of the disabled size is rejected.
                ! CREATE CLUSTER REPLICA test.r2 SIZE '{size}'
                contains:unknown cluster replica size {size}
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
            listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/v26_32_0/no_auth.json",
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

    # Restore for next tests. The recreated database is owned by mz_system, so
    # the materialize role must be re-granted the database-level privileges
    # (notably CREATE, needed to create schemas) and the public schema
    # privileges it holds by default.
    c.sql(
        "CREATE DATABASE materialize",
        port=6877,
        user="mz_system",
    )
    c.sql(
        "GRANT ALL PRIVILEGES ON DATABASE materialize TO materialize",
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
                > ALTER CONNECTION kafka_conn SET (BROKER = 'kafka:9093') WITH (VALIDATE = false);
                > ALTER CONNECTION kafka_conn SET (BROKER = 'kafka:9092') WITH (VALIDATE = true);
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


def workflow_dataflows_without_expression_cache(c: Composition) -> None:
    # Expression-cache enablement is sampled when the catalog opens.
    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_expression_cache": "false",
                "enable_mz_notices": "true",
            },
        )
    ):
        c.up("materialized", Service("testdrive_no_reset", idle=True))
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent("""
                > CREATE TABLE uncached_index_t (a int);
                > INSERT INTO uncached_index_t VALUES (1), (2);
                > CREATE VIEW uncached_index_v AS SELECT a + 1 AS b FROM uncached_index_t;
                > CREATE INDEX uncached_arrangement ON uncached_index_v ();
                > CREATE INDEX IF NOT EXISTS uncached_arrangement ON uncached_index_v (b);
                > CREATE MATERIALIZED VIEW uncached_mv AS SELECT sum(b) AS total FROM uncached_index_v;
                > CREATE DEFAULT INDEX ON uncached_mv;

                # Keep the first refresh pending until after catalog reconstruction.
                > CREATE CLUSTER uncached_refresh SIZE 'scale=1,workers=1', REPLICATION FACTOR 0;
                > CREATE TABLE uncached_refresh_t (a int);
                > INSERT INTO uncached_refresh_t VALUES (1);
                > CREATE MATERIALIZED VIEW uncached_refresh_mv
                  IN CLUSTER uncached_refresh
                  WITH (REFRESH AT CREATION)
                  AS SELECT a FROM uncached_refresh_t;
                > UPDATE uncached_refresh_t SET a = 2;
                """),
        )
        index_id = c.sql_query(
            "SELECT id FROM mz_indexes WHERE name = 'uncached_arrangement'",
            reuse_connection=False,
        )[0][0]

        def verify() -> None:
            c.testdrive(
                service="testdrive_no_reset",
                input=dedent("""
                    > SELECT b FROM uncached_index_v ORDER BY b;
                    2
                    3

                    > SELECT total FROM uncached_mv;
                    5
                    """),
            )
            notices = c.sql_query(
                f"SELECT notice_type, count(*) FROM mz_internal.mz_optimizer_notices "
                f"WHERE object_id = '{index_id}' GROUP BY notice_type",
                user="mz_system",
                port=6877,
                reuse_connection=False,
            )
            assert notices == [("Empty index key", 1)], notices
            plan = c.sql_query(
                "EXPLAIN SELECT b FROM uncached_index_v", reuse_connection=False
            )[0][0]
            assert "uncached_arrangement" in plan, plan
            for stage in ("OPTIMIZED", "PHYSICAL"):
                c.sql_query(
                    f"EXPLAIN {stage} PLAN FOR INDEX uncached_arrangement",
                    reuse_connection=False,
                )
                c.sql_query(
                    f"EXPLAIN {stage} PLAN FOR MATERIALIZED VIEW uncached_mv",
                    reuse_connection=False,
                )

        verify()
        c.kill("materialized")
        c.up("materialized")
        verify()

        c.testdrive(
            service="testdrive_no_reset",
            input=dedent("""
                > INSERT INTO uncached_index_t VALUES (3);
                > SELECT total FROM uncached_mv;
                9

                > ALTER CLUSTER uncached_refresh SET (REPLICATION FACTOR 1);
                > SELECT a FROM uncached_refresh_t;
                2

                # Recovery must use the committed first refresh, not the current input.
                > SELECT a FROM uncached_refresh_mv;
                1
                """),
        )

        c.sql("DROP INDEX uncached_arrangement", reuse_connection=False)
        notices = c.sql_query(
            f"SELECT count(*) FROM mz_internal.mz_optimizer_notices "
            f"WHERE object_id = '{index_id}'",
            user="mz_system",
            port=6877,
            reuse_connection=False,
        )
        assert notices == [(0,)], notices
        c.sql("DROP TABLE uncached_index_t CASCADE", reuse_connection=False)
        c.sql("DROP TABLE uncached_refresh_t CASCADE", reuse_connection=False)
        c.sql("DROP CLUSTER uncached_refresh", reuse_connection=False)


def _catalog_protection_metrics(text: str, shard: str) -> dict:
    names = (
        "mz_persist_shard_diff_size_bytes",
        "mz_persist_shard_cmd_succeeded",
        "mz_persist_shard_usage_current_state_batches_bytes",
        "mz_persist_shard_compaction_applied",
        "mz_persist_shard_batch_part_count",
    )
    series = {}
    for line in text.splitlines():
        if line.split("{", 1)[0] in names and f'shard="{shard}"' in line:
            labels, value = line.rsplit(" ", 1)
            assert labels not in series, labels
            series[labels] = float(value)
    counts = {
        name: sum(key.split("{", 1)[0] == name for key in series) for name in names
    }
    assert all(counts.values()), (shard, counts)
    assert all(math.isfinite(value) for value in series.values()), (shard, series)
    return {
        "series": series,
        "series_counts": counts,
        "totals": {
            name: sum(
                value for key, value in series.items() if key.split("{", 1)[0] == name
            )
            for name in names
        },
    }


def _catalog_committed_update_metrics(text: str) -> dict:
    names = ("mz_catalog_committed_updates", "mz_catalog_committed_update_bytes")
    kinds = ("compaction_bound", "maintained_read_requirement", "other")
    series = {}
    by_kind = {kind: {} for kind in kinds}
    for line in text.splitlines():
        name = line.split("{", 1)[0]
        if name not in names:
            continue
        labels, value = line.rsplit(" ", 1)
        kind_match = re.search(r'(?:\{|,)kind="([^"]+)"(?:,|\})', labels)
        assert kind_match is not None, labels
        kind = kind_match.group(1)
        assert kind in by_kind and name not in by_kind[kind], labels
        number = float(value)
        assert math.isfinite(number), (labels, number)
        series[labels] = number
        by_kind[kind][name] = number
    assert all(set(values) == set(names) for values in by_kind.values()), by_kind
    return {"series": series, "by_kind": by_kind}


def workflow_catalog_read_protection(c: Composition) -> None:
    """Exercise logical recovery protection and persist compaction without cached plans."""
    c.down(destroy_volumes=True)

    def query(sql: str) -> list[tuple]:
        # Observations must not leave a transaction or a blocked peek holding history.
        with c.sql_connection(
            port=6877,
            user="mz_system",
            startup_params={"statement_timeout": "5s"},
        ) as conn:
            cursor = conn.execute(sql.encode())
            return cursor.fetchall() if cursor.description is not None else []

    def td(sql: str, timeout: float = 120) -> None:
        c.testdrive(
            service="testdrive_no_reset",
            input=f"$ set-sql-timeout duration={timeout:.3f}s force=true\n"
            "> SET statement_timeout = '5s';\n" + dedent(sql),
        )

    item_ids: dict[str, str] = {}

    def gid(name: str) -> str:
        [(item_id, global_id)] = query(f"""
            SELECT o.id, g.global_id FROM mz_objects o
            JOIN mz_internal.mz_object_global_ids g ON g.id = o.id
            WHERE o.name = '{name}'
        """)
        item_ids[global_id] = item_id
        return global_id

    def encoded_id(global_id: str) -> dict[str, int]:
        tag = {"s": "System", "u": "User"}[global_id[0]]
        return {tag: int(global_id[1:])}

    def record_sql(kind: str, global_id: str) -> str:
        key = json.dumps(encoded_id(global_id))
        return (
            "SELECT data->'value' FROM mz_internal.mz_catalog_raw "
            f"WHERE data->>'kind' = '{kind}' AND data->'key'->'id' = '{key}'::jsonb"
        )

    def record(kind: str, global_id: str) -> dict:
        [(value,)] = query(record_sql(kind, global_id))
        return value

    def inspect(global_id: str) -> dict:
        return query(f"INSPECT SHARD '{item_ids[global_id]}'")[0][0]

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_catalog_read_protection": "true",
                "enable_expression_cache": "false",
                "enable_logical_compaction_window": "true",
                "enable_index_options": "true",
                # Crash-surviving readers must expire before physical reclamation is observable.
                "persist_reader_lease_duration": "60s",
                # Small batches must exercise real blob compaction, not inline writes.
                "persist_inline_writes_single_max_bytes": "0",
                "persist_compaction_heuristic_min_inputs": "2",
            },
        ),
        Testdrive(
            name="testdrive_no_reset",
            no_reset=True,
            consistent_seed=True,
            materialize_url="postgres://mz_system@materialized:6877",
        ),
    ):
        c.up("materialized", Service("testdrive_no_reset", idle=True))
        td("""
            > SELECT count(*) > 0 FROM mz_internal.mz_catalog_raw
              WHERE data->>'kind' = 'CollectionCompactionBound'
                AND data->'key'->'id' ? 'System';
            true

            > SELECT count(*) FROM mz_internal.mz_catalog_raw m
              WHERE m.data->>'kind' = 'StorageCollectionMetadata'
                AND NOT EXISTS (SELECT 1 FROM mz_internal.mz_catalog_raw b
                  WHERE b.data->>'kind' = 'CollectionCompactionBound'
                    AND b.data->'key'->'id' = m.data->'key'->'id');
            0

            > CREATE CLUSTER protected_refresh SIZE 'scale=1,workers=1', REPLICATION FACTOR 0;
            > CREATE TABLE protected_live (a int) WITH (RETAIN HISTORY = FOR '1s');
            > CREATE TABLE protected_eliminated (a int) WITH (RETAIN HISTORY = FOR '1s');
            > CREATE TABLE protected_control (a int) WITH (RETAIN HISTORY = FOR '1s');
            > INSERT INTO protected_live VALUES (1);
            > INSERT INTO protected_eliminated VALUES (1);
            > INSERT INTO protected_control VALUES (1);
            > CREATE TABLE protected_catalog_probe (a int);
        """)
        td("""
            > CREATE TABLE protected_index_input (a int) WITH (RETAIN HISTORY = FOR '1s');
            > INSERT INTO protected_index_input VALUES (1);
        """)
        index_input = gid("protected_index_input")
        td(f"""
            > SELECT read_frontier::text::numeric > 0
              FROM mz_internal.mz_frontiers WHERE object_id = '{index_input}';
            true
        """)
        physical_deadline = time.monotonic() + 120
        while True:
            physical_since = inspect(index_input)["since"]
            if physical_since and physical_since[0] > 0:
                break
            if time.monotonic() >= physical_deadline:
                raise UIError(
                    f"index input physical since did not advance: {physical_since}"
                )
            time.sleep(0.5)
        assert inspect(index_input)["since"][0] > 0
        query("ALTER SYSTEM SET catalog_read_protection_publish_interval = '1h'")
        unpublished_start = time.monotonic()
        td("""
            > CREATE INDEX protected_index ON protected_index_input (a)
              WITH (RETAIN HISTORY = FOR '1s');
        """)
        index = gid("protected_index")
        index_bound_sql = record_sql("CollectionCompactionBound", index)
        index_frontier_sql = (
            "SELECT read_frontier::text::numeric, write_frontier::text::numeric "
            f"FROM mz_internal.mz_frontiers WHERE object_id = '{index}'"
        )

        def index_permission() -> dict | None:
            response = requests.get(
                f"http://localhost:{c.port('materialized', 6878)}/api/catalog/dump",
                timeout=10,
            )
            response.raise_for_status()
            return response.json()["collection_compaction_bounds"].get(index)

        assert index_permission() is None
        td(f"""
            > SELECT read_frontier > 0 FROM ({index_frontier_sql}) f;
            true
            > SELECT a FROM protected_index_input;
            1
        """)
        assert index_permission() is None
        c.kill("materialized")
        c.up("materialized")
        assert gid("protected_index") == index
        td(f"""
            > SELECT read_frontier > 0 FROM ({index_frontier_sql}) f;
            true
            > SELECT a FROM protected_index_input;
            1
        """)
        assert index_permission() is None
        assert time.monotonic() - unpublished_start < 300
        query("ALTER SYSTEM SET catalog_read_protection_publish_interval = '1s'")
        td(f"""
            > SELECT (v->>'frontier')::numeric > 0
              FROM ({index_bound_sql}) r(v);
            true
            > SELECT read_frontier > 0 FROM ({index_frontier_sql}) f;
            true
        """)
        query("ALTER SYSTEM SET catalog_read_protection_publish_interval = '1h'")
        # Stay well inside the positive cadence while driving progress past the cap.
        # No peek is kept open to retain index history.
        capped_start = time.monotonic()
        published_permission = index_permission()
        assert published_permission is not None
        cap = published_permission["elements"][0]
        for value in range(2, 5):
            query(f"INSERT INTO protected_index_input VALUES ({value})")
            [(read, upper)] = query(index_frontier_sql)
            td(f"""
                > SELECT write_frontier > {max(upper, cap + 1000)},
                         read_frontier <= {cap}
                  FROM ({index_frontier_sql}) f;
                true true
            """)
            assert record("CollectionCompactionBound", index)["frontier"] == cap
            assert index_permission() == {"elements": [cap]}
            assert time.monotonic() - capped_start < 300
        query("ALTER SYSTEM SET catalog_read_protection_publish_interval = '1s'")
        td(f"""
            > SELECT (v->>'frontier')::numeric > {cap}
              FROM ({index_bound_sql}) r(v);
            true
            > SELECT read_frontier > {cap} FROM ({index_frontier_sql}) f;
            true
        """)
        permission = record("CollectionCompactionBound", index)["frontier"]
        c.kill("materialized")
        c.up("materialized")
        assert gid("protected_index") == index
        assert record("CollectionCompactionBound", index)["frontier"] >= permission
        td(f"""
            > SELECT read_frontier >= {permission} FROM ({index_frontier_sql}) f;
            true
            > SELECT a FROM protected_index_input ORDER BY a;
            1
            2
            3
            4
        """)
        plan = query("EXPLAIN SELECT a FROM protected_index_input")[0][0]
        assert "protected_index" in plan, plan
        eliminated_birth = record(
            "CollectionCompactionBound", gid("protected_eliminated")
        )["frontier"]
        td("""
            > CREATE VIEW protected_logical AS
              (SELECT a FROM protected_eliminated UNION ALL SELECT a FROM protected_live)
              EXCEPT ALL SELECT a FROM protected_eliminated;
            > CREATE MATERIALIZED VIEW protected_once IN CLUSTER protected_refresh
              WITH (REFRESH AT CREATION) AS SELECT a FROM protected_logical;
            > CREATE MATERIALIZED VIEW protected_builtin IN CLUSTER protected_refresh
              WITH (REFRESH AT CREATION)
              AS SELECT name FROM mz_catalog.mz_tables WHERE name = 'protected_catalog_probe';
        """)
        ids = {
            name: gid(name)
            for name in (
                "protected_live",
                "protected_eliminated",
                "protected_control",
                "protected_once",
                "protected_builtin",
                "mz_tables",
                "mz_catalog_raw",
            )
        }
        bound_kind = "CollectionCompactionBound"
        requirement_kind = "MaintainedReadRequirement"
        once = record(requirement_kind, ids["protected_once"])
        [(first_refresh,)] = query("""
            SELECT next_refresh::text FROM mz_internal.mz_materialized_view_refreshes r
            JOIN mz_materialized_views v ON v.id = r.materialized_view_id
            WHERE v.name = 'protected_once'
        """)
        first_refresh = int(first_refresh)
        assert (
            isinstance(once["frontier"], int) and once["frontier"] <= first_refresh
        ), once
        td(f"""
            > SELECT last_completed_refresh IS NULL, next_refresh = {first_refresh}::mz_timestamp
              FROM mz_internal.mz_materialized_view_refreshes r
              JOIN mz_materialized_views v ON v.id = r.materialized_view_id
              WHERE v.name = 'protected_once';
            true true
        """)
        expected_inputs = [
            encoded_id(ids[name]) for name in ("protected_live", "protected_eliminated")
        ]
        assert sorted(once["inputs"], key=str) == sorted(expected_inputs, key=str), once
        builtin = record(requirement_kind, ids["protected_builtin"])
        assert builtin["inputs"] == [encoded_id(ids["mz_tables"])], builtin
        for global_id in ids.values():
            assert record(bound_kind, global_id)["frontier"] is not None, global_id

        def verify_plan() -> None:
            plan = query("EXPLAIN OPTIMIZED PLAN FOR MATERIALIZED VIEW protected_once")[
                0
            ][0]
            assert "protected_live" in plan and "protected_eliminated" not in plan, plan
            print(plan)

        control_sql = record_sql(bound_kind, ids["protected_control"])

        def await_state(
            description: str,
            ready: Callable[[], bool],
            timeout: float = 120,
            advance: Callable[[], None] | None = None,
        ) -> None:
            # INSPECT is not composable SQL. Native testdrive Retry waits for each
            # publication advance, then we inspect without acquiring input read holds.
            deadline = time.monotonic() + timeout
            while True:
                control_bound = record(bound_kind, ids["protected_control"])["frontier"]
                if ready():
                    return
                if time.monotonic() >= deadline:
                    sample(f"timeout: {description}")
                    raise UIError(f"timed out waiting for {description}")
                if advance is not None:
                    advance()
                # Persist schedules merges in response to writes. Keep producing real
                # batches while waiting, including after a requirement has completed.
                for _ in range(8):
                    query("UPDATE protected_eliminated SET a = a + 1")
                    query("UPDATE protected_control SET a = a + 1")
                print(f"Waiting for {description}")
                td(
                    f"""
                    > SELECT (v->>'frontier')::numeric > {control_bound}
                      FROM ({control_sql}) AS r(v);
                    true
                    """,
                    timeout=max(1, deadline - time.monotonic()),
                )

        def batches(state: dict) -> list[dict]:
            return [*state["batches"], *state["hollow_batches"].values()]

        def compacted_past(state: dict, timestamp: int) -> bool:
            # A nonempty persisted batch covering the old input, with an advanced
            # batch since, proves a merge was applied. The shard since alone doesn't.
            return any(
                batch["len"] > 0
                and batch["lower"][0] <= timestamp
                and batch["since"]
                and batch["since"][0] > timestamp
                for batch in batches(state)
            )

        def updates(state: dict) -> int:
            return sum(batch["len"] for batch in batches(state))

        def sample(label: str) -> dict:
            history_start = time.monotonic()
            catalog_state = inspect(ids["mz_catalog_raw"])
            catalog_inspection_end = time.monotonic()
            metrics_start = time.monotonic()
            response = requests.get(
                f"http://localhost:{c.port('materialized', 6878)}/metrics", timeout=10
            )
            metrics_end = time.monotonic()
            response.raise_for_status()

            def metrics_for(shard_id: str) -> dict:
                return _catalog_protection_metrics(response.text, shard_id)

            catalog_metrics = metrics_for(catalog_state["shard_id"])
            counters = {
                metric: catalog_metrics["totals"][metric]
                for metric in (
                    "mz_persist_shard_diff_size_bytes",
                    "mz_persist_shard_cmd_succeeded",
                )
            }
            state = {
                "label": label,
                "metrics_start": metrics_start,
                "metrics_end": metrics_end,
                "catalog_inspection_start": history_start,
                "catalog_inspection_end": catalog_inspection_end,
                "catalog_metrics": catalog_metrics,
                "catalog_seqno": catalog_state["seqno"],
                "catalog_counters": counters,
                "collections": {},
            }
            for name, global_id in ids.items():
                inspection_start = time.monotonic()
                shard = inspect(global_id)
                inspection_end = time.monotonic()
                state["collections"][name] = {
                    "inspection_start": inspection_start,
                    "inspection_end": inspection_end,
                    "id": global_id,
                    "bound": record(bound_kind, global_id),
                    "shard_id": shard["shard_id"],
                    "since": shard["since"],
                    "upper": shard["upper"],
                    "environmentd_metrics": metrics_for(shard["shard_id"]),
                    "updates": updates(shard),
                    "persist_upper_minus_since_ms": (
                        shard["upper"][0] - shard["since"][0]
                        if shard["upper"] and shard["since"]
                        else None
                    ),
                    "batches": [
                        {key: batch[key] for key in ("lower", "upper", "since", "len")}
                        for batch in batches(shard)
                    ],
                }
            state["requirements"] = {
                name: record(requirement_kind, ids[name])
                for name in ids
                if name in ("protected_once", "protected_builtin", "protected_ongoing")
            }
            byte_metric = "mz_persist_shard_usage_current_state_batches_bytes"
            state["extra_retained_input_bytes"] = (
                state["collections"]["protected_eliminated"]["environmentd_metrics"][
                    "totals"
                ][byte_metric]
                - state["collections"]["protected_control"]["environmentd_metrics"][
                    "totals"
                ][byte_metric]
            )
            print(json.dumps(state, sort_keys=True))
            return state

        def verify_pending() -> bool:
            assert record(requirement_kind, ids["protected_once"]) == once
            assert record(requirement_kind, ids["protected_builtin"]) == builtin
            assert query("""
                SELECT count(*) FROM mz_cluster_replicas r
                JOIN mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'protected_refresh'
            """) == [(0,)]
            for name in ("protected_live", "protected_eliminated"):
                assert inspect(ids[name])["since"][0] <= first_refresh, name
                assert record(bound_kind, ids[name])["frontier"] <= first_refresh, name
            eliminated = inspect(ids["protected_eliminated"])
            control = inspect(ids["protected_control"])
            # Both tables receive the same revisions. Fewer updates on the control
            # demonstrates consolidation of history that the eliminated input retains.
            checks = {
                "control_since_advanced": control["since"][0] > first_refresh,
                "control_compacted": compacted_past(control, first_refresh),
                "eliminated_compacted": compacted_past(eliminated, eliminated_birth),
                "control_reclaims_more": updates(control) < updates(eliminated),
            }
            print(
                json.dumps(
                    {
                        "probe": checks,
                        "first_refresh": first_refresh,
                        "eliminated_birth": eliminated_birth,
                        "control_since": control["since"],
                        "eliminated_since": eliminated["since"],
                        "control_updates": updates(control),
                        "eliminated_updates": updates(eliminated),
                        "control_batches": [
                            {
                                key: batch[key]
                                for key in ("lower", "upper", "since", "len")
                            }
                            for batch in batches(control)
                        ],
                        "eliminated_batches": [
                            {
                                key: batch[key]
                                for key in ("lower", "upper", "since", "len")
                            }
                            for batch in batches(eliminated)
                        ],
                    }
                ),
                flush=True,
            )
            return all(checks.values())

        verify_plan()
        before = sample("pending-before-writes")
        td("""
            > UPDATE protected_live SET a = 2;
            > UPDATE protected_eliminated SET a = 2;
            > UPDATE protected_control SET a = 2;
            > ALTER TABLE protected_catalog_probe RENAME TO protected_catalog_probe_renamed;
        """)
        await_state("control physically compacted past first refresh", verify_pending)
        after = sample("pending-after-compaction")
        assert (
            before["catalog_metrics"]["series"].keys()
            == after["catalog_metrics"]["series"].keys()
        )
        assert all(
            after["catalog_counters"][metric] >= value
            for metric, value in before["catalog_counters"].items()
        )
        elapsed = after["metrics_end"] - before["metrics_start"]
        # These are measured catalog-shard totals, including builtin maintenance,
        # not an attribution of every persist command to bound publication.
        print(
            json.dumps(
                {
                    "cost_scope": "combined catalog traffic during variable-duration correctness proof, not a cost comparison",
                    "counter_sample_outer_seconds": elapsed,
                    "counter_sample_inner_seconds": after["metrics_start"]
                    - before["metrics_end"],
                    "catalog_counter_deltas": {
                        metric: value - before["catalog_counters"][metric]
                        for metric, value in after["catalog_counters"].items()
                    },
                    "catalog_diff_bytes_per_second_lower_bound": (
                        after["catalog_counters"]["mz_persist_shard_diff_size_bytes"]
                        - before["catalog_counters"]["mz_persist_shard_diff_size_bytes"]
                    )
                    / elapsed,
                },
                sort_keys=True,
            )
        )

        def rejected_while_physically_readable() -> bool:
            permission = record(bound_kind, ids["protected_control"])["frontier"]
            before_since = inspect(ids["protected_control"])["since"][0]
            if before_since >= permission:
                return False
            historical = permission - 1
            td(f"""
                ! CREATE MATERIALIZED VIEW protected_rejected
                  IN CLUSTER protected_refresh WITH (REFRESH AT {historical})
                  AS SELECT a FROM protected_control WHERE false;
                contains: REFRESH AT requested for a time where not all the inputs are readable
            """)
            after_since = inspect(ids["protected_control"])["since"][0]
            # Persist since is monotonic. Both observations must bracket the
            # rejection while H is physically readable, otherwise try another gap.
            print(
                json.dumps(
                    {
                        "admission_timestamp": historical,
                        "permission": permission,
                        "persist_since_before": before_since,
                        "persist_since_after": after_since,
                    }
                )
            )
            return after_since <= historical

        await_state(
            "historical admission rejected during physical compaction lag",
            rejected_while_physically_readable,
        )

        c.kill("materialized")
        c.up("materialized")
        verify_plan()
        assert verify_pending()
        sample("recovered-without-replica")
        td("""
            > ALTER CLUSTER protected_refresh SET (REPLICATION FACTOR 1);
            > SELECT a FROM protected_once;
            1
            > SELECT a FROM protected_live;
            2
            > SELECT name FROM protected_builtin;
            protected_catalog_probe
        """)
        for name in ("protected_once", "protected_builtin"):
            td(f"""
                > SELECT v->>'frontier' IS NULL
                  FROM ({record_sql(requirement_kind, ids[name])}) AS r(v);
                true
            """)
            assert inspect(ids[name])["upper"] == [], name

        def history_released() -> bool:
            eliminated = inspect(ids["protected_eliminated"])
            return (
                eliminated["since"][0] > first_refresh
                and inspect(ids["protected_live"])["since"][0] > first_refresh
                and compacted_past(eliminated, first_refresh)
            )

        await_state("completed requirement releasing input history", history_released)
        sample("completed-without-dropping-mv")

        td("""
            > CREATE MATERIALIZED VIEW protected_ongoing IN CLUSTER protected_refresh
              AS SELECT a FROM protected_logical;
            > SELECT a FROM protected_ongoing;
            2
        """)
        ids["protected_ongoing"] = gid("protected_ongoing")
        ongoing = record(requirement_kind, ids["protected_ongoing"])
        assert sorted(ongoing["inputs"], key=str) == sorted(
            expected_inputs, key=str
        ), ongoing
        start = ongoing["frontier"]
        assert isinstance(start, int), ongoing
        td("""
            > UPDATE protected_live SET a = 3;
            > SELECT a FROM protected_ongoing;
            3
            > SELECT a FROM protected_once;
            1
        """)

        def ongoing_advanced() -> bool:
            eliminated = inspect(ids["protected_eliminated"])
            requirement = record(requirement_kind, ids["protected_ongoing"])
            upper = inspect(ids["protected_ongoing"])["upper"]
            frontier = requirement["frontier"]
            assert frontier is not None and upper, (requirement, upper)
            assert eliminated["since"][0] <= frontier <= upper[0] - 1
            return frontier > start and compacted_past(eliminated, start)

        await_state(
            "ongoing requirement advancing with durable output", ongoing_advanced
        )
        sample("ongoing-not-completed")

        # Pending replacements can make shared-shard registration groups cyclic
        # even though the catalog's query dependency graph is acyclic.
        td("""
            > CREATE MATERIALIZED VIEW protected_a IN CLUSTER protected_refresh
              AS SELECT a FROM protected_live;
            > CREATE MATERIALIZED VIEW protected_b IN CLUSTER protected_refresh
              AS SELECT a FROM protected_live;
            > CREATE REPLACEMENT MATERIALIZED VIEW protected_ar FOR protected_a
              IN CLUSTER protected_refresh AS SELECT a FROM protected_b;
            > CREATE REPLACEMENT MATERIALIZED VIEW protected_br FOR protected_b
              IN CLUSTER protected_refresh AS SELECT a FROM protected_a;
            > CREATE MATERIALIZED VIEW protected_downstream IN CLUSTER protected_refresh
              AS SELECT a FROM protected_a JOIN protected_b USING (a);
            > SELECT a FROM protected_downstream;
            3
        """)
        c.kill("materialized")
        c.up("materialized")
        td("""
            > SELECT a FROM protected_downstream;
            3
            > SELECT a FROM protected_ongoing;
            3
            > SELECT a FROM protected_once;
            1
            > SELECT name FROM protected_builtin;
            protected_catalog_probe
            > DROP MATERIALIZED VIEW protected_ar;
            > DROP MATERIALIZED VIEW protected_br;
        """)

        c.up("kafka")
        td("""
            > CREATE CONNECTION protected_kafka TO KAFKA
              (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            > CREATE CLUSTER protected_ingest SIZE 'scale=1,workers=1';
            > CREATE CLUSTER protected_sink_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 0;
            $ kafka-create-topic topic=protected-source partitions=1
            $ kafka-ingest format=bytes topic=protected-source
            one
            two
            > CREATE SOURCE protected_source IN CLUSTER protected_ingest
              FROM KAFKA CONNECTION protected_kafka
              (TOPIC 'testdrive-protected-source-${testdrive.seed}');
            > ALTER SOURCE protected_source SET (RETAIN HISTORY = FOR '1s');
            > CREATE TABLE protected_export FROM SOURCE protected_source
              (REFERENCE "testdrive-protected-source-${testdrive.seed}")
              FORMAT TEXT WITH (RETAIN HISTORY = FOR '1s');
            > SELECT * FROM protected_export;
            one
            two

            > CREATE TABLE protected_sink_input (a int) WITH (RETAIN HISTORY = FOR '1s');
            > INSERT INTO protected_sink_input VALUES (1);
            > CREATE SINK protected_sink IN CLUSTER protected_sink_cluster
              FROM protected_sink_input INTO KAFKA CONNECTION protected_kafka
              (TOPIC 'protected-sink-${testdrive.seed}') FORMAT JSON ENVELOPE DEBEZIUM;
        """)
        source = gid("protected_source")
        export = gid("protected_export")
        sink = gid("protected_sink")
        sink_input = gid("protected_sink_input")

        def storage_requirement(owner: str, inputs: list[str]) -> dict:
            # Sample input limits before the requirement, and durable output after it.
            # Monotonicity makes these inequalities safe across publication races.
            limits = [
                (record(bound_kind, input_id)["frontier"], inspect(input_id)["since"])
                for input_id in inputs
            ]
            requirement = record(requirement_kind, owner)
            assert sorted(requirement["inputs"], key=str) == sorted(
                [encoded_id(input_id) for input_id in inputs], key=str
            ), requirement
            frontier = requirement["frontier"]
            assert isinstance(frontier, int), requirement
            assert all(
                bound is not None
                and bound <= frontier
                and since
                and since[0] <= frontier
                for bound, since in limits
            ), (requirement, limits)
            return requirement

        def storage_advanced(owner: str, inputs: list[str], start: int) -> bool:
            requirement = storage_requirement(owner, inputs)
            upper = inspect(owner)["upper"]
            print(
                json.dumps({"owner": owner, "requirement": requirement, "upper": upper})
            )
            if requirement["frontier"] <= start:
                return False
            assert upper and requirement["frontier"] <= upper[0] - 1, (
                requirement,
                upper,
            )
            return True

        pending_sink = storage_requirement(sink, [sink, sink_input])
        sink_start = pending_sink["frontier"]
        assert inspect(sink)["upper"] == [0]
        td("""
            > DELETE FROM protected_sink_input;
            > INSERT INTO protected_sink_input VALUES (2);
        """)

        def sink_pending() -> bool:
            assert storage_requirement(sink, [sink, sink_input]) == pending_sink
            assert inspect(sink)["upper"] == [0]
            assert query("""
                SELECT count(*) FROM mz_cluster_replicas r
                JOIN mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'protected_sink_cluster'
            """) == [(0,)]
            return record(bound_kind, ids["protected_control"])["frontier"] > sink_start

        await_state(
            "short retention elapsing with initial sink output pending", sink_pending
        )
        await_state(
            "remap permission advancing before export birth",
            lambda: storage_advanced(source, [source], 0)
            and record(bound_kind, source)["frontier"] > 0,
        )
        td("""
            > ALTER CLUSTER protected_ingest SET (REPLICATION FACTOR 0);
        """)
        remap_permission = record(bound_kind, source)["frontier"]
        remap_since = inspect(source)["since"]
        td("""
            > CREATE TABLE protected_late_export FROM SOURCE protected_source
              (REFERENCE "testdrive-protected-source-${testdrive.seed}")
              FORMAT TEXT WITH (RETAIN HISTORY = FOR '1s');
        """)
        late_export = gid("protected_late_export")
        late_birth = storage_requirement(late_export, [source, late_export])
        birth = late_birth["frontier"]
        assert birth >= remap_permission > 0, (late_birth, remap_permission)
        assert record(bound_kind, late_export)["frontier"] == birth
        assert inspect(late_export)["upper"] == [0]
        # This records physical lag but does not require it. Native SQL read holds
        # constrain catalog permission too, so they cannot pin only physical since.
        print(
            json.dumps(
                {
                    "export_birth": birth,
                    "remap_permission_before_birth": remap_permission,
                    "remap_since_before_birth": remap_since,
                    "remap_since_after_birth": inspect(source)["since"],
                }
            )
        )

        c.kill("materialized")
        c.up("materialized")
        assert sink_pending()
        assert storage_requirement(late_export, [source, late_export]) == late_birth
        assert inspect(late_export)["upper"] == [0]
        td("""
            > ALTER CLUSTER protected_ingest SET (REPLICATION FACTOR 1);
            > ALTER CLUSTER protected_sink_cluster SET (REPLICATION FACTOR 1);
            > SELECT * FROM protected_export;
            one
            two
            > SELECT * FROM protected_late_export;
            one
            two
            $ kafka-verify-data format=json sink=materialize.public.protected_sink key=false sort-messages=true
            {"before": null, "after": {"a": 1}}
            {"before": null, "after": {"a": 2}}
            {"before": {"a": 1}, "after": null}
            $ kafka-ingest format=bytes topic=protected-source
            three
            > SELECT * FROM protected_late_export;
            one
            three
            two
        """)
        await_state(
            "source recovery requirements advancing and remap readers releasing history",
            lambda: storage_advanced(source, [source], birth)
            and storage_advanced(export, [source, export], birth)
            and storage_advanced(late_export, [source, late_export], birth)
            and inspect(late_export)["since"][0] > birth
            and inspect(source)["since"][0] > birth,
            # Ingestion resumption retains its leased reader for 300 seconds.
            timeout=360,
        )
        source_rows = ["one", "two", "three"]

        def drive_remap_compaction() -> None:
            # Empty progress batches can fuse without merging the old nonempty
            # remap batch. New offsets drive merges after the readers release history.
            row = f"compaction-probe-{len(source_rows)}"
            source_rows.append(row)
            td(f"""
                $ kafka-ingest format=bytes topic=protected-source
                {row}
                > SELECT count(*) FROM protected_late_export;
                {len(source_rows)}
            """)

        def verify_source_rows() -> None:
            expected = "\n".join(sorted(source_rows))
            td(
                f"> SELECT * FROM protected_export;\n{expected}\n"
                f"> SELECT * FROM protected_late_export;\n{expected}\n"
            )

        await_state(
            "remap batches physically compacting past export birth",
            lambda: compacted_past(inspect(source), birth),
            advance=drive_remap_compaction,
        )
        verify_source_rows()
        await_state(
            "sink durable progress releasing initial input history",
            lambda: storage_advanced(sink, [sink, sink_input], sink_start)
            and inspect(sink_input)["since"][0] > sink_start,
        )

        td("""
            > CREATE TABLE protected_sink_next (a int) WITH (RETAIN HISTORY = FOR '1s');
            > INSERT INTO protected_sink_next VALUES (99);
        """)
        sink_next = gid("protected_sink_next")
        # A table's physical upper can lag its committed txn-WAL writes. Use the
        # SQL oracle after INSERT to put the ignored row behind the sink's progress.
        [(timestamp,)] = query(
            "EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM protected_sink_next"
        )
        next_snapshot = int(json.loads(timestamp)["determination"]["oracle_read_ts"])
        await_state(
            "sink progress passing the replacement input snapshot",
            lambda: storage_advanced(sink, [sink, sink_input], next_snapshot),
        )
        td("""
            > ALTER SINK protected_sink SET FROM protected_sink_next;
            > ALTER CLUSTER protected_sink_cluster SET (REPLICATION FACTOR 0);
        """)
        before_restart = storage_requirement(sink, [sink, sink_next])
        assert before_restart["frontier"] > next_snapshot
        c.kill("materialized")
        c.up("materialized")
        # Restart fences the old worker before more input arrives. At a stationary
        # upper, publication must reach exactly its predecessor, not just stay below it.
        await_state(
            "recovered paused sink publishing its durable upper predecessor",
            lambda: storage_requirement(sink, [sink, sink_next])["frontier"]
            == inspect(sink)["upper"][0] - 1,
        )
        altered = storage_requirement(sink, [sink, sink_next])
        assert altered["frontier"] >= before_restart["frontier"]
        td(f"""
            > SELECT read_frontier <= {altered['frontier']}::mz_timestamp,
                     write_frontier = {altered['frontier'] + 1}::mz_timestamp
              FROM mz_internal.mz_frontiers WHERE object_id = '{sink}';
            true true
        """)
        td("""
            > INSERT INTO protected_sink_next VALUES (3);
            > INSERT INTO protected_sink_input VALUES (100);
        """)
        await_state(
            "ALTER releasing the old input while protecting the new one",
            lambda: record(bound_kind, sink_input)["frontier"] > altered["frontier"],
        )
        assert storage_requirement(sink, [sink, sink_next]) == altered
        verify_source_rows()
        td("""
            > ALTER CLUSTER protected_sink_cluster SET (REPLICATION FACTOR 1);
            $ kafka-verify-data format=json sink=materialize.public.protected_sink key=false
            {"before": null, "after": {"a": 3}}
            $ kafka-ingest format=bytes topic=protected-source
            four
        """)
        source_rows.append("four")
        verify_source_rows()
        await_state(
            "altered sink advancing after recovery",
            lambda: storage_advanced(sink, [sink, sink_next], altered["frontier"])
            and inspect(sink_next)["since"][0] > altered["frontier"],
        )

        td("""
            > ALTER CLUSTER protected_sink_cluster SET (REPLICATION FACTOR 0);
            > CREATE TABLE protected_no_snapshot_input (a int)
              WITH (RETAIN HISTORY = FOR '1s');
            > INSERT INTO protected_no_snapshot_input VALUES (10);
        """)
        no_snapshot_input = gid("protected_no_snapshot_input")
        [(timestamp,)] = query(
            "EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM protected_no_snapshot_input"
        )
        old_row = int(json.loads(timestamp)["determination"]["oracle_read_ts"])
        fresh_sink = ""
        fresh_requirement: dict = {}
        attempt = 0

        def fresh_sink_during_lag() -> bool:
            nonlocal fresh_sink, fresh_requirement, attempt
            permission = record(bound_kind, no_snapshot_input)["frontier"]
            since_before = inspect(no_snapshot_input)["since"][0]
            if permission <= old_row or since_before >= permission:
                return False
            attempt += 1
            td(f"""
                > CREATE SINK protected_no_snapshot IN CLUSTER protected_sink_cluster
                  FROM protected_no_snapshot_input INTO KAFKA CONNECTION protected_kafka
                  (TOPIC 'protected-no-snapshot-{attempt}-${{testdrive.seed}}')
                  FORMAT JSON ENVELOPE DEBEZIUM WITH (SNAPSHOT = false);
            """)
            fresh_sink = gid("protected_no_snapshot")
            fresh_requirement = storage_requirement(
                fresh_sink, [fresh_sink, no_snapshot_input]
            )
            since_after = inspect(no_snapshot_input)["since"][0]
            permission_after = record(bound_kind, no_snapshot_input)["frontier"]
            assert permission <= fresh_requirement["frontier"] <= permission_after, (
                fresh_requirement,
                permission,
                permission_after,
            )
            assert inspect(fresh_sink)["upper"] == [0]
            print(
                json.dumps(
                    {
                        "fresh_sink_snapshot": False,
                        "permission_before_birth": permission,
                        "permission_after_birth": permission_after,
                        "requirement": fresh_requirement,
                        "since_before_birth": since_before,
                        "since_after_birth": since_after,
                    },
                    sort_keys=True,
                )
            )
            if since_after >= permission:
                # A creation only counts if physical lag brackets admission.
                query("DROP SINK protected_no_snapshot")
                return False
            return True

        await_state(
            "fresh SNAPSHOT=false sink admitted during physical lag",
            fresh_sink_during_lag,
        )
        td("""
            > DELETE FROM protected_no_snapshot_input;
            > INSERT INTO protected_no_snapshot_input VALUES (20);
        """)
        fresh_start = fresh_requirement["frontier"]
        await_state(
            "fresh sink changes pending beyond the retention window",
            lambda: record(bound_kind, ids["protected_control"])["frontier"]
            > fresh_start,
        )
        assert (
            storage_requirement(fresh_sink, [fresh_sink, no_snapshot_input])
            == fresh_requirement
        )
        assert inspect(fresh_sink)["upper"] == [0]
        c.kill("materialized")
        c.up("materialized")
        assert (
            storage_requirement(fresh_sink, [fresh_sink, no_snapshot_input])
            == fresh_requirement
        )
        assert inspect(fresh_sink)["upper"] == [0]
        td("""
            > ALTER CLUSTER protected_sink_cluster SET (REPLICATION FACTOR 1);
            $ kafka-verify-data format=json sink=materialize.public.protected_no_snapshot key=false sort-messages=true
            {"before": null, "after": {"a": 20}}
            {"before": {"a": 10}, "after": null}
        """)
        await_state(
            "fresh sink requirement advancing only after durable output",
            lambda: storage_advanced(
                fresh_sink, [fresh_sink, no_snapshot_input], fresh_start
            )
            and inspect(no_snapshot_input)["since"][0] > fresh_start,
        )
        c.down(destroy_volumes=True)


def workflow_catalog_publication_measurement(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Measure the production catalog path. Requires disposable composition volumes."""
    parser.add_argument(
        "--collection-counts",
        default="100,1000",
        help="Total generated objects per step, tables plus filler views or indexes",
    )
    parser.add_argument("--publication-rounds", type=int, default=3)
    parser.add_argument("--publication-interval-ms", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--active-collections",
        type=int,
        help="Use this many advancing tables and fill the remaining count with --filler-kind",
    )
    parser.add_argument(
        "--filler-kind", choices=("views", "indexes"), default="indexes"
    )
    parser.add_argument(
        "--ddl-rate-hz",
        type=float,
        default=2,
        help="Offered CREATE/DROP pairs per second, zero disables benchmark DDL",
    )
    parser.add_argument("--observer", choices=("poll", "none"), default="poll")
    parser.add_argument("--observation-seconds", type=float, default=10)
    args = parser.parse_args()
    try:
        counts = sorted(set(int(value) for value in args.collection_counts.split(",")))
    except ValueError:
        parser.error("--collection-counts must be comma-separated integers")
    if (
        not counts
        or counts[0] <= 0
        or args.publication_rounds <= 0
        or args.publication_interval_ms <= 0
        or args.timeout_seconds <= 0
        or not math.isfinite(args.observation_seconds)
        or args.observation_seconds <= 0
        or not math.isfinite(args.ddl_rate_hz)
        or args.ddl_rate_hz < 0
        or (
            args.active_collections is not None
            and not 0 < args.active_collections <= counts[0]
        )
    ):
        parser.error(
            "counts, rounds, durations must be positive, DDL rate nonnegative, active collections in 1..min(counts)"
        )

    def query(sql: str) -> list[tuple]:
        with c.sql_connection(
            port=6877,
            user="mz_system",
            startup_params={"statement_timeout": f"{args.timeout_seconds}s"},
        ) as conn:
            cursor = conn.execute(sql.encode())
            return cursor.fetchall() if cursor.description is not None else []

    def execute_each(statements: list[str]) -> None:
        # One connection per batch keeps large sweeps from being dominated by
        # connection setup rather than the catalog path under measurement.
        with c.sql_connection(
            port=6877,
            user="mz_system",
            startup_params={"statement_timeout": f"{args.timeout_seconds}s"},
        ) as conn:
            conn.autocommit = True
            for statement in statements:
                conn.execute(statement.encode())

    def inspect(item_id: str) -> dict:
        start = time.monotonic()
        state = query(f"INSPECT SHARD '{item_id}'")[0][0]
        end = time.monotonic()
        logical_start = time.monotonic()
        logical = query(f"""
            SELECT g.global_id, f.read_frontier::text, f.write_frontier::text
            FROM mz_internal.mz_object_global_ids g
            JOIN mz_internal.mz_frontiers f ON f.object_id = g.global_id
            WHERE g.id = '{item_id}'
            ORDER BY g.global_id
        """)
        logical_end = time.monotonic()
        assert logical, (item_id, "missing logical collection frontiers")
        batches = [*state["batches"], *state["hollow_batches"].values()]
        return {
            "start": start,
            "end": end,
            "logical_frontiers_start": logical_start,
            "logical_frontiers_end": logical_end,
            "logical_frontiers": [
                {
                    "global_id": gid,
                    "read": read,
                    "write": write,
                    "upper_minus_read_ms": (
                        int(write) - int(read)
                        if read is not None and write is not None and int(read) > 0
                        else None
                    ),
                }
                for gid, read, write in logical
            ],
            "state": {
                key: state[key] for key in ("shard_id", "seqno", "since", "upper")
            },
            "updates": sum(batch["len"] for batch in batches),
            "batch_count": len(batches),
            # Transactional tables can have physical uppers behind their logical progress.
            # This signed physical difference is not a table publication-lag measurement.
            "persist_upper_minus_since_ms": (
                state["upper"][0] - state["since"][0]
                if state["upper"] and state["since"]
                else None
            ),
        }

    def metrics_snapshot(shards: dict[str, str]) -> dict:
        start = time.monotonic()
        response = requests.get(
            f"http://localhost:{c.port('materialized', 6878)}/metrics", timeout=10
        )
        end = time.monotonic()
        response.raise_for_status()
        return {
            "start": start,
            "end": end,
            "catalog_committed_updates": _catalog_committed_update_metrics(
                response.text
            ),
            "shards": {
                name: _catalog_protection_metrics(response.text, shard)
                for name, shard in shards.items()
            },
        }

    def catalog_bounds(timeout: float | tuple[float, float] | None = None) -> dict:
        response = requests.get(
            f"http://localhost:{c.port('materialized', 6878)}/api/catalog/dump",
            timeout=timeout if timeout is not None else args.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()["collection_compaction_bounds"]

    def concurrent_ddl(conn, start: float, end: float) -> dict:
        samples = []
        slot = 0
        offered = math.ceil(args.observation_seconds * args.ddl_rate_hz)
        while slot < offered:
            scheduled = start + slot / args.ddl_rate_hz
            time.sleep(max(0, scheduled - time.monotonic()))
            if time.monotonic() >= end:
                break
            sample = {"slot": slot, "scheduled": scheduled, "start": time.monotonic()}
            try:
                conn.execute(b"CREATE VIEW publication_ddl_probe AS SELECT 1")
                sample["create_end"] = time.monotonic()
                sample["drop_start"] = time.monotonic()
                conn.execute(b"DROP VIEW publication_ddl_probe")
            except PsycopgError as error:
                sample["error"] = str(error)
                sample["sqlstate"] = error.sqlstate
            sample["end"] = time.monotonic()
            sample["create_seconds"] = (
                sample.get("create_end", sample["end"]) - sample["start"]
            )
            sample["drop_seconds"] = (
                sample["end"] - sample["drop_start"] if "drop_start" in sample else None
            )
            sample["straddles_observation_end"] = sample["end"] > end
            samples.append(sample)
            if "error" in sample:
                break
            # Drop overdue slots instead of replaying backlog at saturation.
            slot = max(
                slot + 1, math.ceil((time.monotonic() - start) * args.ddl_rate_hz)
            )
        latencies = sorted(
            sample["end"] - sample["start"]
            for sample in samples
            if sample["end"] <= end and "error" not in sample
        )
        return {
            "samples": samples,
            "offered_slots": offered,
            "missed_slots": offered - len(samples),
            "completed_in_window": len(latencies),
            "achieved_pairs_per_second": len(latencies) / args.observation_seconds,
            "in_window_create_drop_percentiles_seconds": {
                f"p{percentile}": latencies[
                    math.ceil(len(latencies) * percentile / 100) - 1
                ]
                for percentile in (50, 95, 99)
                if latencies
            },
        }

    def observe(
        start: float, end: float, target: int, gids: list[str], filler: dict
    ) -> dict:
        samples = []
        publication_seconds = None
        while args.observer == "poll" and time.monotonic() < end:
            request_start = time.monotonic()
            remaining = end - request_start
            if remaining <= 0:
                break
            sample = {
                "start": request_start,
                "budget_seconds": remaining,
                "timeout": False,
            }
            try:
                # Requests timeouts bound connection and socket inactivity, not total
                # response time. Keep the observed overrun, including body/JSON work.
                bounds = catalog_bounds((min(1, remaining / 2), remaining / 2))
                assert_filler(bounds, filler)
                sample["passed_target"] = all(
                    bounds.get(gid, {}).get("elements")
                    and bounds[gid]["elements"][0] > target
                    for gid in gids
                )
            except requests.RequestException as error:
                sample["timeout"] = isinstance(error, requests.Timeout) or isinstance(
                    error.__context__, ReadTimeoutError
                )
                sample["error"] = str(error)
            sample["end"] = time.monotonic()
            sample["overshoot_seconds"] = max(0, sample["end"] - end)
            sample["straddles_observation_end"] = sample["end"] > end
            if (
                sample.get("passed_target")
                and sample["end"] <= end
                and publication_seconds is None
            ):
                publication_seconds = sample["end"] - start
            samples.append(sample)
            time.sleep(max(0, min(0.5, end - time.monotonic())))
        return {"samples": samples, "observed_publication_seconds": publication_seconds}

    def assert_filler(bounds: dict, filler: dict) -> None:
        changed = {
            gid: {"before": bound, "after": bounds.get(gid)}
            for gid, bound in filler.items()
            if bounds.get(gid) != bound
        }
        assert not changed, f"sparse filler bounds are not stable: {changed}"

    c.down(destroy_volumes=True)
    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_catalog_read_protection": "true",
                "enable_expression_cache": "false",
                "enable_logical_compaction_window": "true",
                "max_tables": str(counts[-1]),
                "max_objects_per_schema": str(counts[-1] + 2),
                "catalog_read_protection_publish_interval": f"{args.publication_interval_ms}ms",
                "persist_inline_writes_single_max_bytes": "0",
                "persist_compaction_heuristic_min_inputs": "2",
            }
        ),
        Testdrive(
            name="testdrive_no_reset",
            no_reset=True,
            materialize_url="postgres://mz_system@materialized:6877",
        ),
    ):
        c.up("materialized", Service("testdrive_no_reset", idle=True))
        shared_setup_start = time.monotonic()
        if args.active_collections is not None and args.filler_kind == "indexes":
            execute_each(
                [
                    "CREATE CLUSTER publication_idle SIZE 'scale=1,workers=1', REPLICATION FACTOR 0",
                    "CREATE VIEW publication_constant AS SELECT 1 AS a",
                ]
            )
        shared_setup_end = time.monotonic()
        created = 0
        for count in counts:
            active = args.active_collections or count
            setup_start = time.monotonic()
            # Tables advance without DML. Zero-replica indexes must demonstrate
            # stable governed bounds, while views measure only catalog item size.
            execute_each(
                [
                    statement
                    for i in range(created, count)
                    for statement in (
                        (
                            f"CREATE TABLE publication_{i} (a int) WITH (RETAIN HISTORY = FOR '1s')",
                            f"INSERT INTO publication_{i} VALUES (0)",
                        )
                        if i < active
                        else (
                            (
                                f"CREATE INDEX publication_filler_{i} IN CLUSTER publication_idle ON publication_constant (a)"
                                if args.filler_kind == "indexes"
                                else f"CREATE VIEW publication_filler_{i} AS SELECT {i} AS a"
                            ),
                        )
                    )
                ]
            )
            created = count
            table_rows = query("""
                SELECT t.name, t.id, g.global_id FROM mz_tables t
                JOIN mz_internal.mz_object_global_ids g ON g.id = t.id
                WHERE t.name LIKE 'publication_%'
                ORDER BY substring(t.name, 13)::int
            """)
            assert len(table_rows) == active, (len(table_rows), active)
            filler_rows = query("""
                SELECT o.name, o.id, g.global_id FROM mz_objects o
                LEFT JOIN mz_internal.mz_object_global_ids g ON g.id = o.id
                WHERE o.name LIKE 'publication_filler_%' ORDER BY o.name
            """)
            assert len(filler_rows) == count - active, (
                len(filler_rows),
                count - active,
            )
            filler_gids = (
                [row[2] for row in filler_rows] if args.filler_kind == "indexes" else []
            )
            object_counts = dict(query("""
                SELECT type, count(*) FROM mz_objects
                WHERE name LIKE 'publication_%' GROUP BY type ORDER BY type
            """))
            if args.active_collections is not None and args.filler_kind == "indexes":
                assert query("""
                    SELECT count(*) FROM mz_cluster_replicas r
                    JOIN mz_clusters c ON c.id = r.cluster_id
                    WHERE c.name = 'publication_idle'
                """) == [(0,)]
            deadline = time.monotonic() + args.timeout_seconds
            while True:
                bounds = catalog_bounds()
                [(storage_shard_count,)] = query("""
                    SELECT count(*) FROM mz_internal.mz_storage_shards s
                    JOIN mz_internal.mz_object_global_ids g ON g.global_id = s.object_id
                    JOIN mz_objects o ON o.id = g.id WHERE o.name LIKE 'publication_%'
                """)
                assert storage_shard_count <= active, (storage_shard_count, active)
                if storage_shard_count == active and all(
                    gid in bounds and bounds[gid]["elements"]
                    for gid in [*filler_gids, *(row[2] for row in table_rows)]
                ):
                    break
                if time.monotonic() >= deadline:
                    raise UIError(
                        "generated collections did not acquire nonempty bounds or storage mappings, including zero-replica filler indexes"
                    )
                time.sleep(0.5)
            filler = {gid: bounds[gid] for gid in filler_gids}
            bound_count_before = len(bounds)
            [(catalog_id,)] = query(
                "SELECT id FROM mz_objects WHERE name = 'mz_catalog_raw'"
            )
            sampled_ids = {
                "catalog": catalog_id,
                **{
                    table_rows[i][0]: table_rows[i][1]
                    for i in sorted({0, active // 2, active - 1})
                },
            }
            history_before = {
                name: inspect(item_id) for name, item_id in sampled_ids.items()
            }
            shards = {
                name: sample["state"]["shard_id"]
                for name, sample in history_before.items()
            }
            setup_end = time.monotonic()
            rounds = []
            for revision in range(1, args.publication_rounds + 1):
                update_start = time.monotonic()
                execute_each(
                    [f"UPDATE publication_{i} SET a = a + 1" for i in range(active)]
                )
                [(timestamp,)] = query(
                    "EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM publication_0"
                )
                target = int(json.loads(timestamp)["determination"]["oracle_read_ts"])
                update_end = time.monotonic()
                assert_filler(catalog_bounds(), filler)
                # Open the DDL connection before sampling. Only metrics and the
                # configured load/observer execute within the counter window.
                with (
                    c.sql_connection(
                        port=6877,
                        user="mz_system",
                        startup_params={
                            "statement_timeout": f"{args.timeout_seconds}s"
                        },
                    ) as conn,
                    ThreadPoolExecutor(max_workers=2) as executor,
                ):
                    conn.autocommit = True
                    before = metrics_snapshot(shards)
                    start = time.monotonic()
                    end = start + args.observation_seconds
                    ddl = executor.submit(concurrent_ddl, conn, start, end)
                    observer = executor.submit(
                        observe,
                        start,
                        end,
                        target,
                        [row[2] for row in table_rows],
                        filler,
                    )
                    time.sleep(max(0, end - time.monotonic()))
                    # Sample before joining workers or inspecting shards. A slow
                    # CREATE/DROP or observer may straddle this boundary.
                    after = metrics_snapshot(shards)
                    ddl_result = ddl.result()
                    observer_result = observer.result()
                tail_end = time.monotonic()
                for sample in [*ddl_result["samples"], *observer_result["samples"]]:
                    sample["overlaps_end_metrics_fetch"] = (
                        sample["start"] < after["end"]
                        and sample["end"] > after["start"]
                    )
                assert_filler(catalog_bounds(), filler)
                before_metrics = before["shards"]["catalog"]
                after_metrics = after["shards"]["catalog"]
                assert before_metrics["series"].keys() == after_metrics["series"].keys()
                deltas = {
                    metric: after_metrics["totals"][metric]
                    - before_metrics["totals"][metric]
                    for metric in (
                        "mz_persist_shard_diff_size_bytes",
                        "mz_persist_shard_cmd_succeeded",
                    )
                }
                assert all(delta >= 0 for delta in deltas.values()), deltas
                outer = after["end"] - before["start"]
                inner = after["start"] - before["end"]
                committed_before = before["catalog_committed_updates"]["by_kind"]
                committed_after = after["catalog_committed_updates"]["by_kind"]
                committed_deltas = {
                    kind: {
                        metric: committed_after[kind][metric] - value
                        for metric, value in values.items()
                    }
                    for kind, values in committed_before.items()
                }
                assert all(
                    value >= 0
                    for values in committed_deltas.values()
                    for value in values.values()
                ), committed_deltas
                rounds.append(
                    {
                        "revision": revision,
                        "target_timestamp": target,
                        "update_start": update_start,
                        "update_end": update_end,
                        "observation_start": start,
                        "observation_end": end,
                        "start_sample_lead_seconds": start - before["end"],
                        "end_sample_lateness_seconds": after["start"] - end,
                        "tail_end": tail_end,
                        "ddl": ddl_result,
                        "observer": observer_result,
                        "before": before,
                        "after": after,
                        "combined_catalog_counter_deltas": deltas,
                        "committed_catalog_update_deltas": committed_deltas,
                        "committed_catalog_updates_per_second_bounds": {
                            kind: {
                                metric: [value / outer, value / inner]
                                for metric, value in values.items()
                            }
                            for kind, values in committed_deltas.items()
                        },
                        "counter_sample_outer_seconds": outer,
                        "counter_sample_inner_seconds": inner,
                        "combined_catalog_diff_bytes_per_second_lower_bound": deltas[
                            "mz_persist_shard_diff_size_bytes"
                        ]
                        / outer,
                        "combined_catalog_diff_bytes_per_second_upper_bound": deltas[
                            "mz_persist_shard_diff_size_bytes"
                        ]
                        / inner,
                    }
                )
                if any("error" in sample for sample in ddl_result["samples"]) or any(
                    "error" in sample and not sample["timeout"]
                    for sample in observer_result["samples"]
                ):
                    print(
                        json.dumps({"failed_round": rounds[-1]}, sort_keys=True),
                        flush=True,
                    )
                    raise UIError(
                        "measurement request failed, see failed_round evidence"
                    )
            verification_start = time.monotonic()
            target = max(round_["target_timestamp"] for round_ in rounds)
            while True:
                bounds = catalog_bounds()
                assert_filler(bounds, filler)
                if all(
                    bounds.get(row[2], {}).get("elements")
                    and bounds[row[2]]["elements"][0] > target
                    for row in table_rows
                ):
                    break
                if time.monotonic() - verification_start > args.timeout_seconds:
                    raise UIError(
                        f"publication for {active} collections did not pass {target}"
                    )
                time.sleep(0.5)
            verification_end = time.monotonic()
            history_after = {
                name: inspect(item_id) for name, item_id in sampled_ids.items()
            }
            print(
                json.dumps(
                    {
                        "workflow": "catalog-publication-measurement",
                        "schema_version": 3,
                        "clock": "time.monotonic seconds",
                        "cost_scope": "committed catalog row payload by kind and combined Persist consensus state-metadata traffic",
                        "metric_units": {
                            "mz_catalog_committed_updates": "row updates including retractions",
                            "mz_catalog_committed_update_bytes": "packed catalog rows, excluding timestamps, diffs, compression, and network framing",
                            "mz_persist_shard_diff_size_bytes": "encoded Persist consensus state diffs, not catalog row payload",
                        },
                        "common_observers": "boundary metrics, out-of-window SQL/INSPECT/catalog dump, EXPLAIN TIMESTAMP per update",
                        "generated_object_count": len(table_rows) + len(filler_rows),
                        "advancing_table_count": len(table_rows),
                        "filler_kind": args.filler_kind if filler_rows else None,
                        "filler_count": len(filler_rows),
                        "governed_filler_count": len(filler),
                        "collection_count": len(table_rows) + len(filler),
                        "object_counts_by_type": object_counts,
                        "storage_shard_count": storage_shard_count,
                        "catalog_bound_count_before": bound_count_before,
                        "catalog_bound_count_after": len(bounds),
                        "generated_tables": table_rows,
                        "generated_filler": filler_rows,
                        "filler_bounds": filler,
                        "filler_bounds_after": {
                            gid: bounds[gid] for gid in filler_gids
                        },
                        "observer": args.observer,
                        "ddl_rate_hz": args.ddl_rate_hz,
                        "publication_interval_ms": args.publication_interval_ms,
                        "observation_seconds": args.observation_seconds,
                        "retention_ms": 1000,
                        "setup_start": setup_start,
                        "setup_end": setup_end,
                        "shared_setup_start": shared_setup_start,
                        "shared_setup_end": shared_setup_end,
                        "verification_start": verification_start,
                        "verification_end": verification_end,
                        "history_before": history_before,
                        "history_after": history_after,
                        "rounds": rounds,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        c.down(destroy_volumes=True)


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


def workflow_rename_schema_types_functions(c: Composition) -> None:
    """Verify that ALTER SCHEMA RENAME updates references to a renamed schema's types.

    A type is only ever referenced by a schema-qualified name in "data type"
    position: a cast, a table column type, or a nested element type. Every kind
    of dependent object (view, materialized view, table, another type) reaches
    the type the same way, so all of them must have their create_sql rewritten
    on rename.

    Regression test for three related bugs:

    1. transact.rs RenameSchema only iterated schema.items, missing schema.types
       (and schema.functions). The renamed schema's own types kept stale
       create_sql, which fails to re-parse on restart (the original panic).

    2. transform.rs CreateSqlRewriteSchema never descended into data types, so
       references to a renamed schema's types inside dependents' create_sql
       (casts, column types, element types) were left pointing at the old name.

    3. consistency.rs check_items() only iterated schema.items, so a type with
       invalid create_sql after a rename was never flagged by the checker.

    The persisted create_sql is only re-parsed on boot, so the corruption is
    invisible until a restart, after which the stale references fail to resolve.
    """

    c.up("materialized")

    # Create a schema with a custom type, then exercise every object kind that
    # can reference that type by a schema-qualified name.
    c.sql("CREATE SCHEMA s1")
    c.sql("CREATE TYPE s1.mytype AS LIST (ELEMENT TYPE = int4)")
    # View: references the type in a cast.
    c.sql("CREATE VIEW public.v_uses_type AS SELECT NULL::s1.mytype")
    # Materialized view: same, but persisted as a separate object kind.
    c.sql("CREATE MATERIALIZED VIEW public.mv_uses_type AS SELECT NULL::s1.mytype")
    # Table: references the type as a column type.
    c.sql("CREATE TABLE public.t_uses_type (a s1.mytype)")
    # Type-in-type: an outer type in another schema whose element type is the
    # renamed schema's type (nested data type position).
    c.sql("CREATE TYPE public.outer_type AS LIST (ELEMENT TYPE = s1.mytype)")

    # Sanity: everything works before rename.
    assert c.sql_query("SELECT count(*) FROM public.v_uses_type")[0][0] == 1
    assert c.sql_query("SELECT count(*) FROM public.mv_uses_type")[0][0] == 1
    assert c.sql_query("SELECT count(*) FROM public.t_uses_type")[0][0] == 0

    # Rename the schema.
    c.sql("ALTER SCHEMA s1 RENAME TO s2")

    # Restart Materialize. The persisted create_sql is re-parsed on boot, so any
    # dependent whose create_sql still references the old schema name "s1" (which
    # no longer exists) fails to resolve here.
    c.kill("materialized")
    c.up("materialized")

    # After restart, every dependent must still be queryable.
    assert c.sql_query("SELECT count(*) FROM public.v_uses_type")[0][0] == 1
    assert c.sql_query("SELECT count(*) FROM public.mv_uses_type")[0][0] == 1
    assert c.sql_query("SELECT count(*) FROM public.t_uses_type")[0][0] == 0

    # Every object's create_sql must reference the new schema name, never the old
    # one. This covers the type itself and each kind of dependent.
    checks = [
        ("mz_types", "mytype"),
        ("mz_types", "outer_type"),
        ("mz_views", "v_uses_type"),
        ("mz_materialized_views", "mv_uses_type"),
        ("mz_tables", "t_uses_type"),
    ]
    for catalog_table, name in checks:
        result = c.sql_query(
            f"SELECT create_sql FROM {catalog_table} WHERE name = '{name}'"
        )
        create_sql = result[0][0]
        assert (
            '"s2"' in create_sql and '"s1"' not in create_sql
        ), f"{name} create_sql still references old schema after rename: {create_sql}"

    # Cleanup.
    c.sql("DROP TABLE public.t_uses_type")
    c.sql("DROP MATERIALIZED VIEW public.mv_uses_type")
    c.sql("DROP VIEW public.v_uses_type")
    c.sql("DROP TYPE public.outer_type")
    c.sql("DROP TYPE s2.mytype")
    c.sql("DROP SCHEMA s2")


def workflow_arrangement_sizes_stale_snapshot_after_restart(c: Composition) -> None:
    """After a restart, mz_object_arrangement_size_history should not
    record rows read from stale pre-restart shard contents (SQL-218).

    The collections backing the history snapshots retain pre-restart rows
    until the new introspection subscribes replace them. Each round drops
    two indexes and kills environmentd immediately, before the drops'
    retractions can reach the collections, so the retained shard contents
    include rows for objects that no longer exist in the catalog. After
    the restart nothing can legitimately report those objects, so any
    post-restart history row for them must have been read from the stale
    shard contents. Unlike asserting on sizes, this cannot
    false-positive: a rehydrating index legitimately reports its
    pre-restart size, but a dropped object cannot be reported at all.
    """

    num_replicas = 2
    all_names = [f"sidx{i}" for i in range(1, 21)]

    def name_filter(names: list[str]) -> str:
        return "(" + ", ".join(f"'{n}'" for n in names) + ")"

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
        c.sql(dedent(f"""\
                CREATE CLUSTER stale_test SIZE 'scale=1,workers=1', REPLICATION FACTOR {num_replicas};
                CREATE TABLE stale_t (a int, b text);
                INSERT INTO stale_t SELECT g, repeat('x', 1024) FROM generate_series(1, 30000) g;
                CREATE VIEW stale_v AS SELECT a, b FROM stale_t;
                {"".join(f"CREATE INDEX sidx{i} IN CLUSTER stale_test ON stale_v ((a + {i}));" for i in range(1, 21))}
                """))

        # Object IDs must be captured before dropping: history rows are keyed
        # by object_id, and dropped objects no longer join against mz_objects.
        object_ids = {name: obj_id for obj_id, name in c.sql_query(f"""
                SELECT o.id, o.name FROM mz_objects o
                WHERE o.name IN {name_filter(all_names)}""")}
        assert len(object_ids) == len(all_names)

        def wait_for_full_sample(names: list[str]) -> None:
            expected_count = len(names) * num_replicas
            deadline = time.time() + 120
            while time.time() < deadline:
                if c.sql_query(f"""
                    SELECT 1 FROM mz_internal.mz_object_arrangement_size_history h
                    JOIN mz_objects o ON o.id = h.object_id
                    WHERE o.name IN {name_filter(names)}
                    GROUP BY h.collection_timestamp
                    HAVING count(*) = {expected_count} LIMIT 1"""):
                    return
                time.sleep(0.5)
            raise UIError("timed out waiting for a full sample")

        remaining = all_names
        wait_for_full_sample(remaining)

        for round_num in range(5):
            dropped, remaining = remaining[:2], remaining[2:]

            # Kill right after the drops: their retractions cannot reach the
            # storage collections before the process dies, so the retained
            # shard contents keep rows for the now-nonexistent indexes.
            c.sql(";".join(f"DROP INDEX {name}" for name in dropped))
            c.kill("materialized")
            c.up("materialized")

            # With the freshness gate, recording cannot resume until well
            # after this query runs, so `boundary` cleanly separates pre-kill
            # rows from anything recorded after the restart.
            boundary = c.sql_query("""
                SELECT max(collection_timestamp)::text
                FROM mz_internal.mz_object_arrangement_size_history""")[0][0]
            assert boundary is not None, (
                f"round {round_num}: history table is empty right after "
                "restart; pre-restart contents must be retained"
            )

            # A full post-restart sample of the remaining indexes implies the
            # subscribes have delivered, so the stale window has closed.
            wait_for_full_sample(remaining)

            dropped_ids = ", ".join(f"'{object_ids[name]}'" for name in dropped)
            stale_rows = c.sql_query(f"""
                SELECT h.collection_timestamp::text, h.replica_id, h.object_id, h.size
                FROM mz_internal.mz_object_arrangement_size_history h
                WHERE h.object_id IN ({dropped_ids})
                  AND h.collection_timestamp > '{boundary}'::timestamptz
                ORDER BY h.collection_timestamp""")

            assert not stale_rows, (
                f"round {round_num}: {len(stale_rows)} post-restart history "
                f"rows recorded for indexes dropped just before the restart "
                f"({dropped}); first 10: {stale_rows[:10]}"
            )


def workflow_temporary_item_cleanup(c: Composition) -> None:
    """Temporary tables and views are durable catalog items tagged with the
    UUID of the session that created them (SQL-150), so they need explicit
    cleanup on both paths out of a session.

    Graceful close is handled by the session-close hook, which drops the
    session's items in one catalog transaction. A crash never runs that hook,
    so the items are instead reclaimed the next time the catalog is opened with
    write intent, which fences out every previous owner and therefore every
    session that could still own one.
    """

    def forget_cached_conns() -> None:
        """Drop the connections `sql_query` caches.

        A SIGKILL severs them, and reusing a dead socket surfaces as a spurious
        "server closed the connection unexpectedly" rather than as a retry.
        """
        for conn in c.conns.values():
            try:
                conn.close()
            except Exception:
                pass
        c.conns.clear()

    def query(sql: str) -> list[tuple]:
        try:
            return c.sql_query(sql)
        except OperationalError:
            forget_cached_conns()
            raise

    def wait_for(sql: str, expected: list[tuple], what: str) -> None:
        """Poll until `sql` returns `expected`."""
        deadline = time.time() + 120
        actual = None
        while time.time() < deadline:
            try:
                actual = query(sql)
                if actual == expected:
                    return
            except OperationalError:
                # environmentd is still coming back up.
                pass
            time.sleep(0.5)
        raise UIError(
            f"timed out waiting for {what}: wanted {expected}, last saw {actual}"
        )

    # Temporary items report the temporary schema sentinel '0'.
    temp_item_counts = """
        SELECT
          (SELECT count(*) FROM mz_tables WHERE name = 'tt' AND schema_id = '0'),
          (SELECT count(*) FROM mz_views WHERE name = 'tv' AND schema_id = '0')
    """

    c.down(destroy_volumes=True)
    c.up("materialized")

    # Keep reclamation WAL entries observable until the assertions below.
    c.sql(
        "ALTER SYSTEM SET enable_storage_shard_finalization = false",
        port=6877,
        user="mz_system",
    )

    # Two sessions create temporary items of the same name. Name uniqueness is
    # scoped by the owning session, so both must coexist, and mz_tables and
    # mz_views report every item regardless of owner.
    conn_a = c.sql_connection()
    conn_b = c.sql_connection()
    conn_ids = {}
    for label, conn in (("a", conn_a), ("b", conn_b)):
        cur = conn.cursor()
        cur.execute("SELECT pg_backend_pid()")
        conn_ids[label] = cur.fetchall()[0][0]
        cur.execute("CREATE TEMP TABLE tt (a int)")
        cur.execute("CREATE TEMP VIEW tv AS SELECT * FROM tt")

    wait_for(temp_item_counts, [(2, 2)], "both sessions' temporary items to appear")

    sessions = query(f"""SELECT count(*) FROM mz_internal.mz_sessions
            WHERE connection_id IN ({conn_ids["a"]}, {conn_ids["b"]})""")
    assert sessions == [(2,)], f"both sessions should be in mz_sessions, saw {sessions}"

    # --- Graceful close: only the closing session's items go ------------------

    conn_a.close()

    wait_for(
        temp_item_counts,
        [(1, 1)],
        "session a's temporary items to be dropped and session b's to survive",
    )
    wait_for(
        f"""SELECT count(*) FROM mz_internal.mz_sessions
            WHERE connection_id = {conn_ids["a"]}""",
        [(0,)],
        "session a's mz_sessions row to be retracted",
    )

    # Session b still owns and resolves its own items.
    cur_b = conn_b.cursor()
    cur_b.execute("INSERT INTO tt VALUES (1)")
    cur_b.execute("SELECT count(*) FROM tv")
    assert cur_b.fetchall() == [(1,)], "session b lost its own temporary items"

    # A comment on a temporary item is a durable catalog row too, and item ids
    # are reused, so reclamation must drop it or it can re-attach to an
    # unrelated later object.
    cur_b.execute("COMMENT ON TABLE tt IS 'crash victim'")
    temp_comment_count = """
        SELECT count(*) FROM mz_internal.mz_catalog_raw
        WHERE data->>'kind' = 'Comment'
          AND data->'value'->>'comment' = 'crash victim'
    """
    comments = c.sql_query(temp_comment_count, port=6877, user="mz_system")
    assert comments == [(1,)], f"the temp table's comment was not written: {comments}"

    # Capture the shard backing session b's temp table: the metadata row of
    # the one remaining ephemeral item that has storage (the temp view has
    # none). It is what boot-time reclamation must clean up after the kill.
    shards = c.sql_query(
        """SELECT m.data->'value'->>'shard'
           FROM mz_internal.mz_catalog_raw m
           WHERE m.data->>'kind' = 'StorageCollectionMetadata'
             AND m.data->'key'->'id' IN (
               SELECT i.data->'value'->'global_id'
               FROM mz_internal.mz_catalog_raw i
               WHERE i.data->>'kind' = 'Item'
                 AND i.data->'value'->>'ephemeral_owner_session' IS NOT NULL)""",
        port=6877,
        user="mz_system",
    )
    assert len(shards) == 1, f"expected one ephemeral storage mapping: {shards}"
    temp_shard = shards[0][0]

    # --- kill -9, with session b's items still live ---------------------------

    c.kill("materialized")
    c.up("materialized")
    forget_cached_conns()

    wait_for(
        temp_item_counts,
        [(0, 0)],
        "the crashed session's temporary items to be reclaimed at boot",
    )
    wait_for(
        f"""SELECT count(*) FROM mz_internal.mz_sessions
            WHERE connection_id IN ({conn_ids["a"]}, {conn_ids["b"]})""",
        [(0,)],
        "stale mz_sessions rows to be retracted at boot",
    )

    # mz_tables and mz_views are projections. Only mz_catalog_raw shows whether
    # the durable rows themselves are gone, so a reclamation that merely stopped
    # rendering the items would still be caught here. It is system-only.
    ephemeral = c.sql_query(
        """SELECT count(*) FROM mz_internal.mz_catalog_raw
           WHERE data->>'kind' = 'Item'
             AND data->'value'->>'ephemeral_owner_session' IS NOT NULL""",
        port=6877,
        user="mz_system",
    )
    assert ephemeral == [
        (0,)
    ], f"ephemeral catalog items survived the restart: {ephemeral}"

    # The temp table's storage mapping must have moved to the finalization
    # WAL in the same reclamation, else the metadata row and its persist
    # shard would leak forever.
    metadata = c.sql_query(
        f"""SELECT count(*) FROM mz_internal.mz_catalog_raw
            WHERE data->>'kind' = 'StorageCollectionMetadata'
              AND data->'value'->>'shard' = '{temp_shard}'""",
        port=6877,
        user="mz_system",
    )
    assert metadata == [
        (0,)
    ], f"temp table's storage metadata survived the restart: {temp_shard}"
    unfinalized = c.sql_query(
        f"""SELECT count(*) FROM mz_internal.mz_catalog_raw
            WHERE data->>'kind' = 'UnfinalizedShard'
              AND data->'key'->>'shard' = '{temp_shard}'""",
        port=6877,
        user="mz_system",
    )
    assert unfinalized == [
        (1,)
    ], f"temp table's shard was not enqueued for finalization: {temp_shard}"
    c.sql(
        "ALTER SYSTEM RESET enable_storage_shard_finalization",
        port=6877,
        user="mz_system",
    )

    # The comment row dies with its item.
    comments = c.sql_query(temp_comment_count, port=6877, user="mz_system")
    assert comments == [
        (0,)
    ], f"the temp table's comment survived the restart: {comments}"

    # conn_b's socket died with the process; closing is bookkeeping only.
    try:
        conn_b.close()
    except Exception:
        pass


def workflow_hydration_history_survives_restart(c: Composition) -> None:
    """Durable object and replica hydration rows outlive their writer.

    Killing the service also restarts clusterd, so the replica hydrates again
    and legitimately records fresh episodes: one when rehydration forms a
    single episode, more when the introspection indexes complete before the
    user dataflows install. What must hold is that the pre-restart episodes
    are still there afterwards, unchanged, that every fresh episode starts
    after every pre-restart finish, and that repeated sweeps do not duplicate
    anything. Asserting exact row counts would instead assert on collection
    timing.
    """

    def episodes(name: str = "hydration_history_i") -> list[list]:
        return c.sql_query(f"""
            SELECT h.installed_at::text, h.started_at::text,
                   h.hydrated_at::text, h.status
            FROM mz_internal.mz_object_hydration_history AS h
            JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = h.object_id
            JOIN mz_catalog.mz_objects AS o ON o.id = g.id
            WHERE o.name = '{name}'
            ORDER BY h.installed_at""")

    def replica_episodes() -> list[list]:
        return c.sql_query("""
            SELECT h.replica_id, h.started_at::text, h.finished_at::text,
                   h.object_count::text, h.peak_memory_bytes::text,
                   h.peak_disk_bytes::text, h.status
            FROM mz_internal.mz_replica_hydration_history AS h
            JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
            JOIN mz_catalog.mz_clusters AS c ON c.id = h.cluster_id
            WHERE r.name = 'r1' AND c.name = 'hydration_history'
            ORDER BY h.started_at""")

    def replica_episode_identities(episodes: list[list]) -> list[tuple[str, str]]:
        return [(episode[0], episode[1]) for episode in episodes]

    def parse_ts(text: str) -> datetime:
        return datetime.fromisoformat(text)

    c.down(destroy_volumes=True)
    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "hydration_history_collection_interval": "1s",
                # Pin retention: CI randomizes it, and a short period would
                # prune the episode this test restarts around.
                "hydration_history_retention_period": "30d",
            },
            sanity_restart=False,
        )
    ):
        c.up("materialized")
        c.sql(dedent("""\
            CREATE CLUSTER hydration_history SIZE 'scale=1,workers=2';
            CREATE TABLE hydration_history_t (a int);
            INSERT INTO hydration_history_t SELECT generate_series(1, 100000);
            CREATE INDEX hydration_history_i
                IN CLUSTER hydration_history ON hydration_history_t (a);
            CREATE MATERIALIZED VIEW hydration_history_mv_a
                IN CLUSTER hydration_history AS SELECT a + 1 AS a FROM hydration_history_t;
            CREATE MATERIALIZED VIEW hydration_history_mv_b
                IN CLUSTER hydration_history AS SELECT a + 2 AS a FROM hydration_history_t;
            """))

        deadline = time.time() + 120
        before = []
        while time.time() < deadline:
            before = episodes()
            if before:
                break
            time.sleep(0.5)
        assert (
            len(before) == 1
        ), f"expected exactly one episode, got {before} (empty means it timed out)"

        deadline = time.time() + 120
        replica_before = []
        while time.time() < deadline:
            replica_before = replica_episodes()
            if replica_before:
                break
            time.sleep(0.5)
        assert replica_before, "replica hydration history timed out before restart"

        # Prefer an MV whose persist-sink worker is off worker 0 instead of
        # predicting it from user-ID allocation and hashing. Enough input data
        # separates compute completion from the active worker's durable write,
        # but a single MV is not a reliable trial: its sink can land on worker
        # 0, and a fast snapshot write can collapse both workers' stamps into
        # one logging batch. Either way the separation is unobservable on that
        # MV, so once a trial is fully hydrated and disqualified, create
        # another MV: a fresh id rolls the sink worker and a fresh snapshot
        # write rolls the timing.
        #
        # Which worker holds the maximum is a dice roll, so it must not decide
        # whether the test passes. Every fully hydrated trial is checked
        # against the all-worker maximum below, and that is the property under
        # test whichever worker holds the maximum. A run where no trial
        # separates the workers still checks it, it only loses the ability to
        # tell a worker-0-only implementation apart, and the query shape that
        # separation guards against is pinned deterministically by
        # `collect_requires_every_worker`.
        deadline = time.time() + 240
        mv_names = ["hydration_history_mv_a", "hydration_history_mv_b"]
        max_mvs = 8
        candidates = []
        worker_rows = []
        with c.sql_cursor(reuse_connection=True) as cursor:
            try:
                cursor.execute("SET cluster = hydration_history")
                cursor.execute("SET cluster_replica = r1")
                while time.time() < deadline:
                    name_list = ", ".join(f"'{name}'" for name in mv_names)
                    cursor.execute(f"""
                        SELECT
                            mv.name,
                            max(h.hydrated_at)::text,
                            (max(h.hydrated_at)
                                FILTER (WHERE h.worker_id = 0))::text
                        FROM mz_introspection.mz_compute_hydration_times_per_worker AS h
                        JOIN mz_internal.mz_object_global_ids AS g
                          ON g.global_id = h.export_id
                        JOIN mz_catalog.mz_materialized_views AS mv ON mv.id = g.id
                        WHERE mv.name IN ({name_list})
                        GROUP BY mv.name
                        HAVING count(*) = 2
                           AND count(*) = count(h.hydrated_at)
                        ORDER BY mv.name
                        """.encode())
                    worker_rows = cursor.fetchall()
                    candidates = [
                        row
                        for row in worker_rows
                        if row[1] is not None and row[2] != row[1]
                    ]
                    if candidates:
                        break
                    if len(worker_rows) == len(mv_names) and len(mv_names) < max_mvs:
                        name = f"hydration_history_mv_{chr(ord('a') + len(mv_names))}"
                        cursor.execute(f"""
                            CREATE MATERIALIZED VIEW {name}
                                IN CLUSTER hydration_history
                                AS SELECT a + {len(mv_names) + 1} AS a
                                FROM hydration_history_t
                            """.encode())
                        mv_names.append(name)
                    time.sleep(0.5)
            finally:
                cursor.execute("RESET cluster_replica")
                cursor.execute("RESET cluster")
        assert worker_rows, (
            "no trial materialized view hydrated on every worker, tried "
            f"{len(mv_names)}"
        )

        # Every trial's history episode must use the all-worker maximum, not
        # worker 0's possibly earlier compute-only finish. Separating trials go
        # first so that a regression reports the trial that proves it.
        trials = candidates + [row for row in worker_rows if row not in candidates]
        for mv_name, latest_worker_finish, worker_zero_finish in trials:
            deadline = time.time() + 120
            mv_episodes = []
            while time.time() < deadline:
                mv_episodes = episodes(mv_name)
                if mv_episodes:
                    break
                time.sleep(0.5)
            assert len(mv_episodes) == 1, (
                f"expected one materialized view episode for {mv_name}, "
                f"got {mv_episodes}"
            )
            assert mv_episodes[0][2] == latest_worker_finish, (
                f"durable finish {mv_episodes[0][2]} for {mv_name} did not "
                f"match latest worker finish {latest_worker_finish}. "
                f"worker 0 finished at {worker_zero_finish}"
            )

        # Re-read the pre-restart episodes immediately before the kill. The
        # waits above leave minutes in which a further legitimate episode can
        # be recorded, and the fresh-episode assertions after the restart
        # assume this set is current. Two equal consecutive reads shrink the
        # remaining window to a sweep that starts and commits within one poll
        # gap.
        deadline = time.time() + 120
        replica_before = replica_episodes()
        replica_before_settled = False
        while time.time() < deadline:
            time.sleep(2.0)
            current = replica_episodes()
            replica_before_settled = current == replica_before
            if replica_before_settled:
                break
            replica_before = current
        assert (
            replica_before_settled
        ), f"pre-restart replica episodes did not settle: {replica_before}"
        replica_before_ids = replica_episode_identities(replica_before)
        replica_before_started_at = {identity[1] for identity in replica_before_ids}
        assert len(replica_before_ids) == len(
            set(replica_before_ids)
        ), f"duplicate replica hydration identities before restart: {replica_before}"
        latest_before_finish = max(parse_ts(episode[2]) for episode in replica_before)

        c.kill("materialized")
        c.up("materialized")

        # The pre-restart episode must remain byte-identical, and restarting the
        # replica must produce exactly one episode with a fresh installation.
        deadline = time.time() + 120
        after = []
        fresh = []
        while time.time() < deadline:
            after = episodes()
            fresh = [episode for episode in after if episode[0] != before[0][0]]
            if before[0] in after and len(fresh) == 1:
                break
            time.sleep(0.5)
        assert (
            before[0] in after
        ), f"restart lost the pre-restart episode: had {before}, now {after}"
        assert (
            len(after) == 2 and len(fresh) == 1
        ), f"expected one preserved and one fresh episode, got {after}"

        deadline = time.time() + 120
        replica_after = []
        replica_after_ids = []
        replica_fresh_ids = set()
        while time.time() < deadline:
            replica_after = replica_episodes()
            replica_after_ids = replica_episode_identities(replica_after)
            replica_fresh_ids = set(replica_after_ids) - set(replica_before_ids)
            if set(replica_before_ids) <= set(replica_after_ids) and replica_fresh_ids:
                break
            time.sleep(0.5)
        assert all(
            episode in replica_after for episode in replica_before
        ), f"restart changed replica episodes: had {replica_before}, now {replica_after}"
        assert set(replica_before_ids) <= set(
            replica_after_ids
        ), f"restart lost replica episodes: had {replica_before}, now {replica_after}"
        # Rehydration can record one fresh episode or several: the
        # introspection indexes can finish before the user dataflows install,
        # forming an earlier disconnected episode that is recorded on its own.
        # The monotonic history guard orders them all after pre-restart
        # history.
        assert (
            replica_fresh_ids
        ), f"restart did not produce a fresh replica identity: {replica_after}"
        assert all(
            parse_ts(identity[1]) > latest_before_finish
            for identity in replica_fresh_ids
        ), f"fresh replica episode overlaps pre-restart history: {replica_after}"
        assert all(
            identity[1] not in replica_before_started_at
            for identity in replica_fresh_ids
        ), f"restart reused a replica hydration start: {replica_after}"
        assert len(replica_after_ids) == len(
            set(replica_after_ids)
        ), f"restart produced duplicate replica hydration identities: {replica_after}"

        # Let several sweeps run. The pre-restart episodes must not be
        # duplicated, and everything recorded since the restart must stay
        # ordered after them.
        time.sleep(10)
        settled = episodes()
        assert (
            before[0] in settled
        ), f"pre-restart episode disappeared: had {before}, now {settled}"
        assert len(settled) == len(
            set(tuple(row) for row in settled)
        ), f"sweeps duplicated a hydration episode: {settled}"
        assert len(settled) == 2, f"expected two settled episodes, got {settled}"

        replica_settled = replica_episodes()
        replica_settled_ids = replica_episode_identities(replica_settled)
        assert len(replica_settled_ids) == len(
            set(replica_settled_ids)
        ), f"sweeps duplicated a replica hydration identity: {replica_settled}"
        assert set(replica_before_ids) <= set(
            replica_settled_ids
        ), f"pre-restart replica episodes disappeared: {replica_settled}"
        assert all(
            episode in replica_settled for episode in replica_before
        ), f"pre-restart replica episodes changed: {replica_settled}"
        assert replica_fresh_ids <= set(
            replica_settled_ids
        ), f"post-restart replica episodes disappeared: {replica_settled}"
        assert all(
            parse_ts(identity[1]) > latest_before_finish
            for identity in set(replica_settled_ids) - set(replica_before_ids)
        ), f"settled replica episodes overlap pre-restart history: {replica_settled}"


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name in ("default", "catalog-publication-measurement"):
            return

        with c.test_case(name):
            c.workflow(name)

    files = buildkite.shard_list(list(c.workflows.keys()), lambda workflow: workflow)
    c.test_parts(files, process)
