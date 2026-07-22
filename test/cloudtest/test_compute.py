# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import copy
import json
import logging
import time

import pytest
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet
from materialize.cloudtest.util.cluster import cluster_pod_name, cluster_service_name
from materialize.cloudtest.util.exists import exists, not_exists
from materialize.cloudtest.util.wait import wait
from materialize.mzcompose import cluster_replica_size_map

LOGGER = logging.getLogger(__name__)


def test_cluster_sizing(mz: MaterializeApplication) -> None:
    """Test that a SIZE N cluster indeed creates N clusterd instances."""
    SIZE = 2

    mz.environmentd.sql(
        f"CREATE CLUSTER sized1 REPLICAS (sized_replica1 (SIZE 'scale={SIZE},workers=1'))"
    )
    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert cluster_id is not None

    replica_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_cluster_replicas WHERE name = 'sized_replica1'"
    )[0][0]
    assert replica_id is not None

    for compute_id in range(0, SIZE):
        compute_pod = cluster_pod_name(cluster_id, replica_id, compute_id)
        wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")


@pytest.mark.parametrize(
    "failpoint",
    ["", "after_catalog_drop_replica=panic", "after_sequencer_drop_replica=panic"],
)
@pytest.mark.skip(
    reason="Failpoints mess up the Mz instance https://linear.app/materializeinc/issue/CLU-127"
)
def test_cluster_shutdown(mz: MaterializeApplication, failpoint: str) -> None:
    """Test that dropping a cluster or replica causes the associated clusterds to shut down."""

    LOGGER.info(f"Testing cluster shutdown with failpoint={failpoint}")

    mz.set_environmentd_failpoints(failpoint)

    def sql_expect_crash(sql: str) -> None:
        # We expect executing `sql` will crash environmentd. To ensure it is actually `sql`
        # wait until the SQL interface is available.
        mz.wait_for_sql()
        try:
            mz.environmentd.sql(sql)
        except InterfaceError as e:
            LOGGER.error(f"Expected SQL error: {e}")

    mz.environmentd.sql(
        "CREATE CLUSTER shutdown1 REPLICAS (shutdown_replica1 (SIZE 'scale=1,workers=1'), shutdown_replica2 (SIZE 'scale=1,workers=1'))"
    )

    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'shutdown1'"
    )[0][0]
    assert cluster_id is not None

    compute_pods = {}
    compute_svcs = {}
    for replica_name in ["shutdown_replica1", "shutdown_replica2"]:
        replica_id = mz.environmentd.sql_query(
            f"SELECT id FROM mz_cluster_replicas WHERE name = '{replica_name}'"
        )[0][0]
        assert replica_id is not None

        compute_pod = cluster_pod_name(cluster_id, replica_id)
        compute_pods[replica_name] = compute_pod
        wait(condition="condition=Ready", resource=compute_pod)

        compute_svc = cluster_service_name(cluster_id, replica_id)
        compute_svcs[replica_name] = compute_svc
        exists(resource=compute_svc)

    sql_expect_crash("DROP CLUSTER REPLICA shutdown1.shutdown_replica1")
    wait(condition="delete", resource=compute_pods["shutdown_replica1"])
    not_exists(resource=compute_svcs["shutdown_replica1"])

    sql_expect_crash("DROP CLUSTER shutdown1 CASCADE")
    wait(condition="delete", resource=compute_pods["shutdown_replica2"])
    not_exists(resource=compute_svcs["shutdown_replica2"])

    mz.set_environmentd_failpoints("")


def get_value_from_label(
    mz: MaterializeApplication, cluster_id: str, replica_id: str, jsonpath: str
) -> str:
    return mz.kubectl(
        "get",
        "pods",
        f"--selector=cluster.environmentd.materialize.cloud/cluster-id={cluster_id},cluster.environmentd.materialize.cloud/replica-id={replica_id}",
        "-o",
        "jsonpath='{.items[*]." + jsonpath + "}'",
    )


def get_node_selector(
    mz: MaterializeApplication, cluster_id: str, replica_id: str
) -> str:
    return get_value_from_label(mz, cluster_id, replica_id, "spec.nodeSelector")


def test_disk_label(mz: MaterializeApplication) -> None:
    """Test that cluster replicas have the correct materialize.cloud/disk labels"""

    mz.environmentd.sql("CREATE CLUSTER disk SIZE = 'scale=1,workers=2'")

    cluster_id, replica_id = mz.environmentd.sql_query(
        "SELECT mz_clusters.id, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'disk'"
    )[0]
    assert cluster_id is not None
    assert replica_id is not None

    node_selectors = get_node_selector(mz, cluster_id, replica_id)
    assert node_selectors == '\'{"materialize.cloud/disk":"true"}\'', node_selectors

    mz.environmentd.sql("DROP CLUSTER disk CASCADE")


def test_cluster_replica_sizes(mz: MaterializeApplication) -> None:
    """Test that --cluster-replica-sizes mapping is respected"""
    # Some time for existing cluster drops to complete so we don't try to spin them up again
    time.sleep(5)
    # Start from the real replica-size map used at bootstrap. The default
    # cluster and the builtin system clusters are created with the `bootstrap`
    # size and persisted in the catalog, so they must still resolve after we
    # restart environmentd below. Restarting with a map that drops their size
    # makes environmentd panic in catalog apply. Overlay two custom sizes that
    # carry node `selectors`, which is the mapping this test exercises.
    size_map = cluster_replica_size_map()
    size_map["small"] = {
        "scale": 1,
        "workers": 1,
        "cpu_limit": None,
        "memory_limit": None,
        "disk_limit": None,
        "credits_per_hour": "1",
        "disabled": False,
        "selectors": {"key1": "value1"},
    }
    size_map["medium"] = {
        "scale": 1,
        "workers": 1,
        "cpu_limit": None,
        "memory_limit": None,
        "disk_limit": None,
        "credits_per_hour": "1",
        "disabled": False,
        "selectors": {"key2": "value2", "key3": "value3"},
    }

    stateful_set = [
        resource
        for resource in mz.resources
        if type(resource) == EnvironmentdStatefulSet
    ]
    assert len(stateful_set) == 1
    stateful_set = copy.deepcopy(stateful_set[0])
    stateful_set.env["MZ_CLUSTER_REPLICA_SIZES"] = json.dumps(size_map)
    stateful_set.replace()
    mz.wait_for_sql()

    for key, value in {
        "small": size_map["small"],
        "medium": size_map["medium"],
    }.items():
        mz.environmentd.sql(f"CREATE CLUSTER scale_{key} MANAGED, SIZE = '{key}'")
        cluster_id, replica_id = mz.environmentd.sql_query(
            f"SELECT mz_clusters.id, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'scale_{key}'"
        )[0]
        assert cluster_id is not None
        assert replica_id is not None

        expected = value.get("selectors", {}) | {"materialize.cloud/disk": "true"}
        # get_node_selector wraps the kubectl jsonpath output in single quotes,
        # so a pod that has not appeared in the Kubernetes API yet yields the
        # two-character string "''", not an empty string. Strip the quotes
        # before deciding whether the nodeSelector is populated, otherwise the
        # retry below breaks on the first attempt and json.loads chokes on an
        # empty value.
        node_selectors_stripped = ""
        for i in range(1, 10):
            node_selectors_stripped = get_node_selector(
                mz, cluster_id, replica_id
            ).strip("'")
            if node_selectors_stripped:
                break
            print("No node selectors available yet, sleeping")
            time.sleep(5)
        node_selectors = json.loads(node_selectors_stripped)
        assert (
            node_selectors == expected
        ), f"actual: {node_selectors}, but expected {expected}"

        mz.environmentd.sql(f"DROP CLUSTER scale_{key} CASCADE")

    # Cleanup
    stateful_set = [
        resource
        for resource in mz.resources
        if type(resource) == EnvironmentdStatefulSet
    ]
    assert len(stateful_set) == 1
    stateful_set = stateful_set[0]
    stateful_set.replace()
    mz.wait_for_sql()
