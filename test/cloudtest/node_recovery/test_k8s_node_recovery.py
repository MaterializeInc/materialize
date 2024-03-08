# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import time
from dataclasses import dataclass
from textwrap import dedent

import pytest

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)

TD_TIMEOUT_SHORT = 45
TD_TIMEOUT_FULL_RECOVERY = 600


@dataclass
class ReplicaDefinition:
    cluster_name: str
    index: int
    availability_zone: str

    def get_name(self) -> str:
        return f"{self.cluster_name}_r_{self.index}"


@dataclass
class ClusterDefinition:
    name: str
    replica_definitions: list[ReplicaDefinition]

    def create_replica_definitions_sql(self) -> str:
        replica_definitions = []
        for replica in self.replica_definitions:
            replica_definitions.append(
                f"{replica.get_name()} (SIZE = '1', AVAILABILITY ZONE '{replica.availability_zone}')"
            )

        return ", ".join(replica_definitions)


def default_compute_cluster() -> ClusterDefinition:
    """Single cluster in availability zone 1."""
    compute_cluster = ClusterDefinition("c_compute", [])
    compute_cluster.replica_definitions.append(
        ReplicaDefinition(compute_cluster.name, index=1, availability_zone="1")
    )
    return compute_cluster


def default_storage_cluster() -> ClusterDefinition:
    """Single cluster in availability zone 2."""
    storage_cluster = ClusterDefinition("c_storage", [])
    storage_cluster.replica_definitions.append(
        ReplicaDefinition(storage_cluster.name, index=1, availability_zone="2")
    )
    return storage_cluster


REQUIRED_PROGRESS = 5


def populate(
    mz: MaterializeApplication,
    compute_cluster: ClusterDefinition,
    storage_cluster: ClusterDefinition,
) -> None:
    all_clusters = [compute_cluster, storage_cluster]

    drop_cluster_statements = [
        f"> DROP CLUSTER IF EXISTS {cluster.name};" for cluster in all_clusters
    ]
    # string needs the same indentation as testdrive script below
    drop_cluster_statement_sql = "\n".join(drop_cluster_statements)

    create_cluster_statements = [
        f"> CREATE CLUSTER {cluster.name} REPLICAS ({cluster.create_replica_definitions_sql()});"
        for cluster in all_clusters
    ]
    # string needs the same indentation as testdrive script below
    create_cluster_statement_sql = "\n".join(create_cluster_statements)

    mz.testdrive.run(
        input=dedent(
            """
            > DROP MATERIALIZED VIEW IF EXISTS mv;
            > DROP SOURCE IF EXISTS source CASCADE;
            """
        )
        + dedent(drop_cluster_statement_sql)
        + "\n"
        + dedent(create_cluster_statement_sql)
        + "\n"
        + dedent(
            f"""
            > SELECT name FROM mz_clusters WHERE name IN ('{compute_cluster.name}', '{storage_cluster.name}');
            {storage_cluster.name}
            {compute_cluster.name}

            > CREATE SOURCE source IN CLUSTER {storage_cluster.name}
              FROM LOAD GENERATOR COUNTER
              (TICK INTERVAL '500ms');

            > SELECT COUNT(*) FROM (SHOW SOURCES) WHERE name = 'source';
            1

            > CREATE MATERIALIZED VIEW mv (f1) IN CLUSTER {compute_cluster.name} AS SELECT counter + 1 FROM source;

            > CREATE DEFAULT INDEX IN CLUSTER {compute_cluster.name} ON mv;

            > SELECT COUNT(*) > 0 from mv;
            true
            """
        ),
        no_reset=True,
    )


def validate_state(
    mz: MaterializeApplication,
    reached_index: int,
    must_exceed_reached_index: bool,
    timeout_in_sec: int,
    expected_state: str,
    isolation_level: str = "STRICT SERIALIZABLE",
) -> None:
    comparison_operator = ">" if must_exceed_reached_index else ">="
    print(f"Expect '{expected_state}' within timeout of {timeout_in_sec}s")

    testdrive_run_timeout_in_sec = 10

    validation_succeeded = False
    last_error_message = None

    start_time = time.time()

    # re-run testdrive to make sure it connects to the most recent envd
    max_run_count = int(timeout_in_sec / testdrive_run_timeout_in_sec)
    max_run_count = 1 if max_run_count < 1 else max_run_count
    for run in range(0, max_run_count):
        is_last_run = run + 1 == max_run_count
        try:
            mz.testdrive.run(
                input=dedent(
                    f"""
                    > SET TRANSACTION_ISOLATION TO '{isolation_level}';

                    > SELECT COUNT(*) {comparison_operator} {reached_index} FROM source; -- validate source with isolation {isolation_level}
                    true

                    > SELECT COUNT(*) {comparison_operator} {reached_index} FROM mv; -- validate mv with isolation {isolation_level}
                    true
                    """
                ),
                default_timeout=f"{testdrive_run_timeout_in_sec}s",
                no_reset=True,
                suppress_command_error_output=not is_last_run,
            )
            validation_succeeded = True
            break
        except Exception as e:
            run_info = f"{run}/{max_run_count}"
            # arbitrary error can occur if envd is not yet ready after restart
            if is_last_run:
                print(f"Error occurred in run {run_info}, aborting!")
            else:
                print(f"Error occurred in run {run_info}, retrying.")
            last_error_message = str(e)

    end_time = time.time()

    if not validation_succeeded:
        raise FailedTestExecutionError(
            [
                TestFailureDetails(
                    message=f"Failed to achieve '{expected_state}' using '{isolation_level}' within {timeout_in_sec}s!",
                    details=last_error_message,
                )
            ]
        )

    duration = round(end_time - start_time, 1)
    print(
        f"Succeeded to achieve '{expected_state}' within {duration} seconds (limit: {timeout_in_sec}s)"
    )


def get_current_counter_index(mz: MaterializeApplication) -> int:
    """
    This query has no timeout. Only use it if is expected to deliver.
    """
    reached_value: int = mz.environmentd.sql_query("SELECT COUNT(*) FROM source")[0][0]
    return reached_value


def suspend_node_of_replica(
    mz: MaterializeApplication, cluster: ClusterDefinition
) -> str:
    node_names = mz.get_cluster_node_names(cluster.name)
    assert len(node_names) > 0
    print(f"Cluster {cluster.name} uses nodes {node_names}")

    suspended_node_name = node_names[0]
    mz.suspend_k8s_node(suspended_node_name)
    return suspended_node_name


@pytest.mark.node_recovery
def test_unreplicated_storage_cluster_on_failing_node(
    mz: MaterializeApplication,
) -> None:
    """
    An unreplicated storage cluster is on the failed node. Queries of a downstream index in serializable mode should
    continue to work but return stale data. Staleness should resolve within a minute or two.
    """
    compute_cluster = default_compute_cluster()
    storage_cluster = default_storage_cluster()

    populate(mz, compute_cluster, storage_cluster)
    reached_index = get_current_counter_index(mz)

    suspended_node_name = suspend_node_of_replica(mz, storage_cluster)

    # with SERIALIZABLE
    validate_state(
        mz,
        reached_index,
        must_exceed_reached_index=False,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="stale data being delivered timely",
        isolation_level="SERIALIZABLE",
    )

    # with STRICT SERIALIZABLE
    validate_state(
        mz,
        reached_index,
        must_exceed_reached_index=False,
        timeout_in_sec=TD_TIMEOUT_FULL_RECOVERY,
        expected_state="data being delivered",
        isolation_level="STRICT SERIALIZABLE",
    )

    # only request this index because the previous validation succeeded / did not block
    stalled_index = get_current_counter_index(mz)

    # expect live data to be delivered at most after two minutes in production (or longer in k8s)
    validate_state(
        mz,
        stalled_index,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_FULL_RECOVERY,
        expected_state="live data after to node recovery",
    )

    recovered_index = get_current_counter_index(mz)

    mz.revive_suspended_k8s_node(suspended_node_name)

    validate_state(
        mz,
        recovered_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="no issues after node recovery",
    )


@pytest.mark.node_recovery
def test_unreplicated_compute_cluster_on_failing_node(
    mz: MaterializeApplication,
) -> None:
    """
    An unreplicated compute cluster is on the failed node. Queries of indexes on the compute cluster should fail, but
    resolve within a minute or two.
    """
    compute_cluster = default_compute_cluster()
    storage_cluster = default_storage_cluster()

    populate(mz, compute_cluster, storage_cluster)
    reached_index = get_current_counter_index(mz)

    suspended_node_name = suspend_node_of_replica(
        mz,
        compute_cluster,
    )

    # expect (live) data to be delivered after at most after two minutes in production (or longer in k8s)
    validate_state(
        mz,
        reached_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_FULL_RECOVERY,
        expected_state="node recovery and live data",
    )

    recovered_index = get_current_counter_index(mz)

    mz.revive_suspended_k8s_node(suspended_node_name)

    validate_state(
        mz,
        recovered_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="no issues after node recovery",
    )


@pytest.mark.node_recovery
def test_replicated_compute_cluster_on_failing_node(mz: MaterializeApplication) -> None:
    """
    A replicated compute cluster is on the failed node. Queries of indexes on the compute cluster should experience no
    disruption in latency, thanks to the second replica.
    """
    compute_cluster = default_compute_cluster()
    compute_cluster.replica_definitions.append(
        ReplicaDefinition(compute_cluster.name, index=2, availability_zone="3")
    )
    assert (
        compute_cluster.replica_definitions[0].availability_zone
        != compute_cluster.replica_definitions[1].availability_zone
    ), "Test configuration error"
    storage_cluster = default_storage_cluster()

    populate(mz, compute_cluster, storage_cluster)
    reached_index = get_current_counter_index(mz)

    nodes_with_compute_clusters = set(mz.get_cluster_node_names(compute_cluster.name))
    nodes_with_storage_clusters = set(mz.get_cluster_node_names(storage_cluster.name))
    nodes_with_only_compute_clusters = (
        nodes_with_compute_clusters - nodes_with_storage_clusters
    )
    assert (
        len(nodes_with_only_compute_clusters) > 0
    ), "No nodes that do not contain both compute and storage clusters"

    suspended_node_name = next(iter(nodes_with_only_compute_clusters))
    print(
        f"Compute clusters on nodes {nodes_with_compute_clusters}, storage clusters on nodes {nodes_with_storage_clusters}"
    )
    print(f"Suspending {suspended_node_name}")
    mz.suspend_k8s_node(suspended_node_name)

    validate_state(
        mz,
        reached_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="live data without disruption in latency",
        isolation_level="SERIALIZABLE",
    )

    reached_index = get_current_counter_index(mz)

    mz.revive_suspended_k8s_node(suspended_node_name)

    validate_state(
        mz,
        reached_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="no issues after node recovery",
    )


@pytest.mark.node_recovery
def test_envd_on_failing_node(mz: MaterializeApplication) -> None:
    """
    environmentd is on the failed node. All connections should fail, but resolve within a minute or two.
    """
    compute_cluster = default_compute_cluster()
    storage_cluster = default_storage_cluster()

    populate(mz, compute_cluster, storage_cluster)
    reached_index = get_current_counter_index(mz)

    envd_node_name = mz.get_k8s_value(
        "app=environmentd", "{.items[*].spec.nodeName}", remove_quotes=True
    )
    mz.suspend_k8s_node(envd_node_name)

    print("Expecting connection timeout...")
    # all connections / queries should fail initially
    try:
        mz.testdrive.run(
            input=dedent(
                """
                > SELECT COUNT(*) > 0 FROM mz_tables;
                true
                """
            ),
            default_timeout=f"{TD_TIMEOUT_SHORT}s",
            no_reset=True,
            suppress_command_error_output=True,
        )
        assert False, "Expected timeout"
    except Exception:
        # OK
        print("Timeout is expected")

    print("Survived connection timeout.")

    # expect (live) data to be delivered after at most after two minutes in production (or longer in k8s)
    validate_state(
        mz,
        reached_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_FULL_RECOVERY,
        expected_state="node recovery and live data",
    )

    recovered_index = get_current_counter_index(mz)

    mz.revive_suspended_k8s_node(envd_node_name)

    validate_state(
        mz,
        recovered_index + REQUIRED_PROGRESS,
        must_exceed_reached_index=True,
        timeout_in_sec=TD_TIMEOUT_SHORT,
        expected_state="no issues after node recovery",
    )
