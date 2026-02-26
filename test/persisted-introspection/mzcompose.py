# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for persist-backed compute introspection sources."""

import time
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
]


def workflow_default(c: Composition) -> None:
    """Run all test workflows."""

    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_sql_syntax(c: Composition) -> None:
    """Test that PERSIST INTROSPECTION option is accepted in CREATE/ALTER CLUSTER."""
    c.up("materialized")

    # CREATE CLUSTER with PERSIST INTROSPECTION = true
    c.sql(
        "CREATE CLUSTER pi_true (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # CREATE CLUSTER with PERSIST INTROSPECTION = false
    c.sql(
        "CREATE CLUSTER pi_false (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = false)"
    )

    # CREATE CLUSTER without the option (defaults to false)
    c.sql("CREATE CLUSTER pi_default (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)")

    # ALTER CLUSTER to toggle the option
    c.sql("ALTER CLUSTER pi_false SET (PERSIST INTROSPECTION = true)")
    c.sql("ALTER CLUSTER pi_true SET (PERSIST INTROSPECTION = false)")

    # RESET the option
    c.sql("ALTER CLUSTER pi_default SET (PERSIST INTROSPECTION = true)")
    c.sql("ALTER CLUSTER pi_default RESET (PERSIST INTROSPECTION)")

    # Clean up
    c.sql("DROP CLUSTER pi_true CASCADE")
    c.sql("DROP CLUSTER pi_false CASCADE")
    c.sql("DROP CLUSTER pi_default CASCADE")

    c.kill("materialized")


def workflow_dyncfg_kill_switch(c: Composition) -> None:
    """Test that the dyncfg kill switch controls persist introspection behavior."""
    c.up("materialized")

    # Disable the global kill switch.
    c.sql(
        "ALTER SYSTEM SET enable_persist_introspection = false",
        port=6877,
        user="mz_system",
    )

    # Creating a cluster with PERSIST INTROSPECTION = true should still succeed,
    # even when the dyncfg is off. The dyncfg gates the runtime behavior, not
    # the SQL syntax.
    c.sql(
        "CREATE CLUSTER pi_dyncfg (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # Verify the cluster is functional.
    c.sql("SET CLUSTER = pi_dyncfg")
    c.sql("CREATE TABLE t_dyncfg (x int)")
    c.sql("CREATE MATERIALIZED VIEW mv_dyncfg AS SELECT count(*) FROM t_dyncfg")
    _wait_for_view(c, "mv_dyncfg")
    result = c.sql_query("SELECT * FROM mv_dyncfg")
    assert result[0][0] == 0, f"expected 0, got {result[0][0]}"

    # Re-enable the kill switch.
    c.sql(
        "ALTER SYSTEM SET enable_persist_introspection = true",
        port=6877,
        user="mz_system",
    )

    # The cluster should remain functional after re-enabling.
    c.sql("INSERT INTO t_dyncfg VALUES (1), (2), (3)")
    _wait_for_view(c, "mv_dyncfg", expected_count=3)
    result = c.sql_query("SELECT * FROM mv_dyncfg")
    assert result[0][0] == 3, f"expected 3, got {result[0][0]}"

    # Clean up.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP CLUSTER pi_dyncfg CASCADE")
    c.sql("DROP TABLE t_dyncfg CASCADE")

    c.kill("materialized")


def workflow_cluster_lifecycle(c: Composition) -> None:
    """Test creating, using, and dropping clusters with persist introspection."""
    c.up("materialized")

    # Create a cluster with persist introspection enabled.
    c.sql(
        "CREATE CLUSTER lifecycle_test (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # Create objects on the cluster.
    c.sql("SET CLUSTER = lifecycle_test")
    c.sql("CREATE TABLE t_lifecycle (x int)")
    c.sql(
        "CREATE MATERIALIZED VIEW mv_lifecycle AS SELECT count(*) AS cnt FROM t_lifecycle"
    )

    # Insert data and verify the MV works.
    c.sql("INSERT INTO t_lifecycle VALUES (1), (2), (3)")
    _wait_for_view(c, "mv_lifecycle", expected_count=3)
    result = c.sql_query("SELECT cnt FROM mv_lifecycle")
    assert result[0][0] == 3, f"expected 3, got {result[0][0]}"

    # Verify introspection data is flowing by checking that the cluster has
    # dataflow operators (via the standard introspection subscribe path).
    c.sql("SET CLUSTER = quickstart")
    replica_count = c.sql_query(
        dedent(
            """
            SELECT count(*)
            FROM mz_cluster_replicas r
            JOIN mz_clusters c ON r.cluster_id = c.id
            WHERE c.name = 'lifecycle_test'
            """
        )
    )
    assert replica_count[0][0] == 1, f"expected 1 replica, got {replica_count[0][0]}"

    # Drop the cluster with CASCADE.
    c.sql("DROP TABLE t_lifecycle CASCADE")
    c.sql("DROP CLUSTER lifecycle_test CASCADE")

    # Verify the cluster is gone.
    result = c.sql_query(
        "SELECT count(*) FROM mz_clusters WHERE name = 'lifecycle_test'"
    )
    assert result[0][0] == 0, "cluster should be dropped"

    # Recreate with the same name to verify no stale state.
    c.sql(
        "CREATE CLUSTER lifecycle_test (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )
    c.sql("SET CLUSTER = lifecycle_test")
    c.sql("CREATE TABLE t_lifecycle2 (x int)")
    c.sql("INSERT INTO t_lifecycle2 VALUES (42)")
    c.sql(
        "CREATE MATERIALIZED VIEW mv_lifecycle2 AS SELECT count(*) AS cnt FROM t_lifecycle2"
    )
    _wait_for_view(c, "mv_lifecycle2", expected_count=1)
    result = c.sql_query("SELECT cnt FROM mv_lifecycle2")
    assert result[0][0] == 1, f"expected 1, got {result[0][0]}"

    # Clean up.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP TABLE t_lifecycle2 CASCADE")
    c.sql("DROP CLUSTER lifecycle_test CASCADE")

    c.kill("materialized")


def workflow_replica_restart(c: Composition) -> None:
    """Test that a replica restart leaves data in a consistent state.

    After dropping and recreating the replica, old introspection data from the
    previous replica should not linger. The MV persist sink's self-correction
    mechanism retracts stale data on restart.
    """
    c.up("materialized")

    # Create a cluster with persist introspection.
    c.sql(
        "CREATE CLUSTER restart_test (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # Populate with data.
    c.sql("SET CLUSTER = restart_test")
    c.sql("CREATE TABLE t_restart (x int)")
    c.sql("INSERT INTO t_restart VALUES (1), (2), (3)")
    c.sql(
        "CREATE MATERIALIZED VIEW mv_restart AS SELECT count(*) AS cnt FROM t_restart"
    )
    _wait_for_view(c, "mv_restart", expected_count=3)

    # Verify MV returns correct data before restart.
    result = c.sql_query("SELECT cnt FROM mv_restart")
    assert result[0][0] == 3, f"expected 3 before restart, got {result[0][0]}"

    # Simulate a replica restart by scaling down and back up.
    # This drops the old replica and creates a new one.
    c.sql("ALTER CLUSTER restart_test SET (REPLICATION FACTOR 0)")

    # Verify the cluster has no replicas.
    c.sql("SET CLUSTER = quickstart")
    replica_count = c.sql_query(
        dedent(
            """
            SELECT count(*)
            FROM mz_cluster_replicas r
            JOIN mz_clusters c ON r.cluster_id = c.id
            WHERE c.name = 'restart_test'
            """
        )
    )
    assert replica_count[0][0] == 0, f"expected 0 replicas, got {replica_count[0][0]}"

    # Scale back up. A new replica is created with fresh persist sinks.
    c.sql("ALTER CLUSTER restart_test SET (REPLICATION FACTOR 1)")

    # Wait for the replica to become ready.
    _wait_for_replica_online(c, "restart_test")

    # Verify the MV returns correct data after restart. The self-correction
    # mechanism ensures the new replica's persist sinks retract any stale data
    # and write fresh data.
    c.sql("SET CLUSTER = restart_test")
    _wait_for_view(c, "mv_restart", expected_count=3)
    result = c.sql_query("SELECT cnt FROM mv_restart")
    assert result[0][0] == 3, f"expected 3 after restart, got {result[0][0]}"

    # Insert more data to verify the cluster is fully operational after restart.
    c.sql("INSERT INTO t_restart VALUES (4), (5)")
    _wait_for_view(c, "mv_restart", expected_count=5)
    result = c.sql_query("SELECT cnt FROM mv_restart")
    assert result[0][0] == 5, f"expected 5 after insert, got {result[0][0]}"

    # Clean up.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP TABLE t_restart CASCADE")
    c.sql("DROP CLUSTER restart_test CASCADE")

    c.kill("materialized")


def workflow_catalog_changes(c: Composition) -> None:
    """Test that adding/removing clusters with persist introspection has the
    expected catalog effects."""
    c.up("materialized")

    # Create a cluster with persist introspection.
    c.sql(
        "CREATE CLUSTER cat_test (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # The cluster should appear in mz_clusters.
    result = c.sql_query(
        "SELECT name, managed FROM mz_clusters WHERE name = 'cat_test'"
    )
    assert len(result) == 1, f"expected 1 cluster, got {len(result)}"
    assert result[0][0] == "cat_test"
    assert result[0][1] is True  # managed

    # Verify the cluster has a replica.
    replicas = c.sql_query(
        dedent(
            """
            SELECT r.name
            FROM mz_cluster_replicas r
            JOIN mz_clusters c ON r.cluster_id = c.id
            WHERE c.name = 'cat_test'
            """
        )
    )
    assert len(replicas) == 1, f"expected 1 replica, got {len(replicas)}"

    # ALTER CLUSTER to toggle persist introspection off.
    c.sql("ALTER CLUSTER cat_test SET (PERSIST INTROSPECTION = false)")

    # The cluster should still exist and be managed.
    result = c.sql_query(
        "SELECT name, managed FROM mz_clusters WHERE name = 'cat_test'"
    )
    assert len(result) == 1
    assert result[0][1] is True

    # ALTER CLUSTER to change size (triggers replica replacement).
    c.sql("ALTER CLUSTER cat_test SET (SIZE 'scale=2,workers=2')")
    _wait_for_replica_online(c, "cat_test")

    # Verify replica still exists after size change.
    replicas = c.sql_query(
        dedent(
            """
            SELECT r.name
            FROM mz_cluster_replicas r
            JOIN mz_clusters c ON r.cluster_id = c.id
            WHERE c.name = 'cat_test'
            """
        )
    )
    assert len(replicas) == 1, f"expected 1 replica after resize, got {len(replicas)}"

    # Drop the cluster.
    c.sql("DROP CLUSTER cat_test CASCADE")

    # Verify complete removal from catalog.
    result = c.sql_query("SELECT count(*) FROM mz_clusters WHERE name = 'cat_test'")
    assert result[0][0] == 0, "cluster should be removed from catalog"

    result = c.sql_query(
        dedent(
            """
            SELECT count(*)
            FROM mz_cluster_replicas r
            JOIN mz_clusters c ON r.cluster_id = c.id
            WHERE c.name = 'cat_test'
            """
        )
    )
    assert result[0][0] == 0, "replicas should be removed from catalog"

    c.kill("materialized")


def workflow_multiple_clusters(c: Composition) -> None:
    """Test that multiple clusters with persist introspection can coexist."""
    c.up("materialized")

    # Create two clusters with persist introspection.
    c.sql(
        "CREATE CLUSTER multi_a (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )
    c.sql(
        "CREATE CLUSTER multi_b (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # Create tables and MVs on each cluster.
    c.sql("CREATE TABLE t_multi (x int)")
    c.sql("INSERT INTO t_multi VALUES (1), (2)")

    c.sql("SET CLUSTER = multi_a")
    c.sql("CREATE MATERIALIZED VIEW mv_a AS SELECT count(*) AS cnt FROM t_multi")

    c.sql("SET CLUSTER = multi_b")
    c.sql("CREATE MATERIALIZED VIEW mv_b AS SELECT sum(x) AS total FROM t_multi")

    # Verify both MVs produce correct results.
    c.sql("SET CLUSTER = multi_a")
    _wait_for_view(c, "mv_a", expected_count=2)
    result = c.sql_query("SELECT cnt FROM mv_a")
    assert result[0][0] == 2, f"expected 2, got {result[0][0]}"

    c.sql("SET CLUSTER = multi_b")
    _wait_for_view(c, "mv_b", expected_total=3)
    result = c.sql_query("SELECT total FROM mv_b")
    assert result[0][0] == 3, f"expected 3, got {result[0][0]}"

    # Drop one cluster; the other should be unaffected.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP CLUSTER multi_a CASCADE")

    c.sql("SET CLUSTER = multi_b")
    result = c.sql_query("SELECT total FROM mv_b")
    assert result[0][0] == 3, f"expected 3 after dropping multi_a, got {result[0][0]}"

    # Clean up.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP TABLE t_multi CASCADE")
    c.sql("DROP CLUSTER multi_b CASCADE")

    c.kill("materialized")


def workflow_environmentd_restart(c: Composition) -> None:
    """Test that persist introspection clusters survive an environmentd restart."""
    c.up("materialized")

    # Create a cluster with persist introspection and populate data.
    c.sql(
        "CREATE CLUSTER restart_env (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )
    c.sql("CREATE TABLE t_env (x int)")
    c.sql("INSERT INTO t_env VALUES (10), (20), (30)")
    c.sql("SET CLUSTER = restart_env")
    c.sql("CREATE MATERIALIZED VIEW mv_env AS SELECT count(*) AS cnt FROM t_env")
    _wait_for_view(c, "mv_env", expected_count=3)

    # Restart environmentd.
    c.kill("materialized")
    c.up("materialized")

    # Verify the cluster and data survived.
    c.sql("SET CLUSTER = restart_env")
    _wait_for_view(c, "mv_env", expected_count=3)
    result = c.sql_query("SELECT cnt FROM mv_env")
    assert result[0][0] == 3, f"expected 3 after restart, got {result[0][0]}"

    # Verify the cluster is still managed with persist introspection.
    c.sql("SET CLUSTER = quickstart")
    result = c.sql_query("SELECT managed FROM mz_clusters WHERE name = 'restart_env'")
    assert result[0][0] is True

    # Clean up.
    c.sql("DROP TABLE t_env CASCADE")
    c.sql("DROP CLUSTER restart_env CASCADE")

    c.kill("materialized")


def workflow_per_replica_views(c: Composition) -> None:
    """Test that per-replica introspection views are created and queryable."""
    c.up("materialized")

    # Enable the dyncfg so persist introspection actually creates schemas.
    c.sql(
        "ALTER SYSTEM SET enable_persist_introspection = true",
        port=6877,
        user="mz_system",
    )

    # Create a cluster with persist introspection enabled.
    c.sql(
        "CREATE CLUSTER view_test (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1, PERSIST INTROSPECTION = true)"
    )

    # Wait for the replica to be online.
    _wait_for_replica_online(c, "view_test")

    # Look up the per-replica schema name. Schema names use IDs, not cluster
    # names: mz_introspection_{cluster_id}_{replica_id}.
    schema_row = c.sql_query(
        dedent(
            """
            SELECT s.name
            FROM mz_schemas s
            JOIN mz_clusters c ON s.name LIKE 'mz_introspection_' || c.id || '_%'
            WHERE c.name = 'view_test'
            """
        )
    )
    assert len(schema_row) == 1, f"expected 1 per-replica schema, got {len(schema_row)}"
    schema_name = schema_row[0][0]

    # Query a per-replica view (mz_dataflows aggregates mz_dataflows_per_worker).
    # Create some dataflows to ensure the view has data.
    c.sql("SET CLUSTER = view_test")
    c.sql("CREATE TABLE t_views (x int)")
    c.sql("CREATE MATERIALIZED VIEW mv_views AS SELECT count(*) FROM t_views")
    _wait_for_view(c, "mv_views", expected_count=0)

    # Query a derived view (mz_dataflows) in the per-replica schema.
    result = c.sql_query(f"SELECT count(*) FROM materialize.{schema_name}.mz_dataflows")
    assert result[0][0] >= 0, f"expected non-negative count, got {result[0][0]}"

    # Query a base per-worker view.
    result = c.sql_query(
        f"SELECT count(*) FROM materialize.{schema_name}.mz_dataflows_per_worker"
    )
    assert result[0][0] >= 0, f"expected non-negative count, got {result[0][0]}"

    # Query mz_arrangement_sizes which chains multiple views.
    result = c.sql_query(
        f"SELECT count(*) FROM materialize.{schema_name}.mz_arrangement_sizes"
    )
    assert result[0][0] >= 0, f"expected non-negative count, got {result[0][0]}"

    # Drop the cluster and verify the schema is removed.
    c.sql("SET CLUSTER = quickstart")
    c.sql("DROP TABLE t_views CASCADE")
    c.sql("DROP CLUSTER view_test CASCADE")
    schema_count = c.sql_query(
        f"SELECT count(*) FROM mz_schemas WHERE name = '{schema_name}'"
    )
    assert schema_count[0][0] == 0, "per-replica schema should be dropped"

    c.kill("materialized")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_for_view(
    c: Composition,
    view_name: str,
    expected_count: int | None = None,
    expected_total: int | None = None,
    timeout_secs: int = 30,
) -> None:
    """Poll a materialized view until it returns the expected result."""
    deadline = time.time() + timeout_secs
    last_result = None
    while time.time() < deadline:
        try:
            if expected_count is not None:
                result = c.sql_query(f"SELECT * FROM {view_name}")
                last_result = result
                if result and result[0][0] == expected_count:
                    return
            elif expected_total is not None:
                result = c.sql_query(f"SELECT * FROM {view_name}")
                last_result = result
                if result and result[0][0] == expected_total:
                    return
            else:
                # Just check the view is queryable.
                c.sql_query(f"SELECT * FROM {view_name}")
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(
        f"View {view_name} did not reach expected state within {timeout_secs}s. "
        f"Last result: {last_result}"
    )


def _wait_for_replica_online(
    c: Composition, cluster_name: str, timeout_secs: int = 60
) -> None:
    """Wait until all replicas of a cluster report 'ready' status."""
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            result = c.sql_query(
                dedent(
                    f"""
                    SELECT count(*)
                    FROM mz_internal.mz_cluster_replica_statuses s
                    JOIN mz_cluster_replicas r ON s.replica_id = r.id
                    JOIN mz_clusters c ON r.cluster_id = c.id
                    WHERE c.name = '{cluster_name}' AND s.status = 'online'
                    """
                )
            )
            if result and result[0][0] > 0:
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Replicas of cluster {cluster_name} did not become ready within {timeout_secs}s"
    )
