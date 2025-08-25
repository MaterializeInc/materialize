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
@pytest.mark.skip(reason="Failpoints mess up the Mz instance database-issues#5263")
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

    for value in ("true", "false"):
        mz.environmentd.sql(
            f"CREATE CLUSTER disk_{value} MANAGED, SIZE = 'scale=1,workers=2,legacy', DISK = {value}"
        )

        (cluster_id, replica_id) = mz.environmentd.sql_query(
            f"SELECT mz_clusters.id, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'disk_{value}'"
        )[0]
        assert cluster_id is not None
        assert replica_id is not None

        node_selectors = get_node_selector(mz, cluster_id, replica_id)
        if value == "true":
            assert (
                node_selectors == '\'{"materialize.cloud/disk":"true"}\''
            ), node_selectors
        else:
            assert node_selectors == "''"

        mz.environmentd.sql(f"DROP CLUSTER disk_{value} CASCADE")


@pytest.mark.skip(reason="Keeps flaking, see database-issues#8299")
def test_cluster_replica_sizes(mz: MaterializeApplication) -> None:
    """Test that --cluster-replica-sizes mapping is respected"""
    # Some time for existing cluster drops to complete so we don't try to spin them up again
    time.sleep(5)
    cluster_replica_size_map = {
        "small": {
            "scale": 1,
            "workers": 1,
            "cpu_limit": None,
            "memory_limit": None,
            "disk_limit": None,
            "credits_per_hour": "1",
            "disabled": False,
            "selectors": {"key1": "value1"},
        },
        "medium": {
            "scale": 1,
            "workers": 1,
            "cpu_limit": None,
            "memory_limit": None,
            "disk_limit": None,
            "credits_per_hour": "1",
            "disabled": False,
            "selectors": {"key2": "value2", "key3": "value3"},
        },
        # for existing clusters
        "1": {
            "scale": 1,
            "workers": 1,
            "cpu_limit": None,
            "memory_limit": None,
            "disk_limit": None,
            "disabled": False,
            "credits_per_hour": "1",
        },
        "2-1": {
            "scale": 2,
            "workers": 2,
            "cpu_limit": None,
            "memory_limit": None,
            "disk_limit": None,
            "disabled": False,
            "credits_per_hour": "2",
        },
    }

    stateful_set = [
        resource
        for resource in mz.resources
        if type(resource) == EnvironmentdStatefulSet
    ]
    assert len(stateful_set) == 1
    stateful_set = copy.deepcopy(stateful_set[0])
    stateful_set.env["MZ_CLUSTER_REPLICA_SIZES"] = json.dumps(cluster_replica_size_map)
    stateful_set.extra_args.append("--bootstrap-default-cluster-replica-size=1")
    stateful_set.replace()
    mz.wait_for_sql()

    for key, value in {
        "small": cluster_replica_size_map["small"],
        "medium": cluster_replica_size_map["medium"],
    }.items():
        mz.environmentd.sql(f"CREATE CLUSTER scale_{key} MANAGED, SIZE = '{key}'")
        (cluster_id, replica_id) = mz.environmentd.sql_query(
            f"SELECT mz_clusters.id, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'scale_{key}'"
        )[0]
        assert cluster_id is not None
        assert replica_id is not None

        expected = value.get("selectors", {}) | {"materialize.cloud/disk": "true"}
        node_selectors_raw = ""
        for i in range(1, 10):
            node_selectors_raw = get_node_selector(mz, cluster_id, replica_id)
            if node_selectors_raw:
                break
            print("No node selectors available yet, sleeping")
            time.sleep(5)
        node_selectors = json.loads(node_selectors_raw[1:-1])
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
