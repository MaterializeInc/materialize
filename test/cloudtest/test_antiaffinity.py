# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import pytest

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.cluster import cluster_pod_name
from materialize.cloudtest.util.wait import wait


def zones_used(
    mz: MaterializeApplication,
    replica_names: list[str] | None = None,
    cluster_name: str = "antiaffinity_cluster1",
) -> int:
    if replica_names is None:
        replica_names = [
            "antiaffinity_replica1",
            "antiaffinity_replica2",
            "antiaffinity_replica3",
        ]
    nodes = {}

    for replica_name in replica_names:

        cluster_id = mz.environmentd.sql_query(
            f"SELECT id FROM mz_clusters WHERE name = '{cluster_name}'"
        )[0][0]
        assert cluster_id is not None

        replica_id = mz.environmentd.sql_query(
            "SELECT id FROM mz_cluster_replicas "
            f"WHERE cluster_id = '{cluster_id}' AND name = '{replica_name}'"
        )[0][0]
        assert replica_id is not None

        cluster_pod = cluster_pod_name(cluster_id, replica_id)

        wait(condition="condition=Ready", resource=cluster_pod)

        compute_pod = mz.environmentd.api().read_namespaced_pod(
            cluster_pod.removeprefix("pod/"),
            mz.environmentd.namespace(),
        )
        spec = compute_pod.spec
        assert spec is not None
        assert spec.node_name is not None

        node = mz.environmentd.api().read_node(spec.node_name)
        assert node is not None
        assert node.metadata is not None
        assert isinstance(node.metadata.labels, dict)

        zone = node.metadata.labels["materialize.cloud/availability-zone"]
        nodes[zone] = 1

    return len(nodes.keys())


def test_create_cluster_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple replicas as defined in CREATE CLUSTER are placed in different availability zones."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS (
            antiaffinity_replica1 (SIZE 'scale=1,workers=1'),
            antiaffinity_replica2 (SIZE 'scale=1,workers=1'),
            antiaffinity_replica3 (SIZE 'scale=1,workers=1')
        )"""
    )

    assert zones_used(mz) == 3

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


def test_create_cluster_replica_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple replicas as created with CREATE CLUSTER REPLICA are placed in different availability zones."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS ();
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica1 SIZE 'scale=1,workers=1';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica2 SIZE 'scale=1,workers=1';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica3 SIZE 'scale=1,workers=1';
        """
    )

    assert zones_used(mz) == 3

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


def test_create_cluster_replica_zone_specified(mz: MaterializeApplication) -> None:
    """Test that the AVAILABILITY ZONE argument to CREATE CLUSTER REPLICA is observed."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS ();
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica1 SIZE 'scale=1,workers=1' , AVAILABILITY ZONE '3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica2 SIZE 'scale=1,workers=1' , AVAILABILITY ZONE '3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica3 SIZE 'scale=1,workers=1' , AVAILABILITY ZONE '3';
        """
    )

    assert zones_used(mz) == 1

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


def test_create_cluster_replica_zone_mixed(mz: MaterializeApplication) -> None:
    """Test that the AVAILABILITY ZONE argument to CREATE CLUSTER REPLICA is observed."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS ();
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica1 SIZE 'scale=1,workers=1' , AVAILABILITY ZONE '3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica2 SIZE 'scale=1,workers=1' , AVAILABILITY ZONE '3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica3 SIZE 'scale=1,workers=1';
        """
    )

    assert zones_used(mz) == 2

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


def test_managed_set_azs(mz: MaterializeApplication) -> None:
    """Test that the AVAILABILITY ZONE argument to CREATE CLUSTER REPLICA is observed."""

    mz.environmentd.sql(
        "ALTER SYSTEM SET enable_managed_cluster_availability_zones = true",
        port="internal",
        user="mz_system",
    )

    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 3, AVAILABILITY ZONES ('1', '3')
        """
    )

    assert (
        zones_used(
            mz,
            replica_names=[
                "r1",
                "r2",
                "r3",
            ],
        )
        == 2
    )

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


@pytest.mark.skip(reason="Not currently guaranteed by implementation.")
def test_create_clusters_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple independent clusters are spread out to different availability zones."""

    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS (
            antiaffinity_replica1 (SIZE 'scale=1,workers=1')
        );
        CREATE CLUSTER antiaffinity_cluster2 REPLICAS (
            antiaffinity_replica2 (SIZE 'scale=1,workers=1')
        );
        CREATE CLUSTER antiaffinity_cluster3 REPLICAS (
            antiaffinity_replica3 (SIZE 'scale=1,workers=1')
        );
        """
    )

    assert zones_used(mz) == 3

    mz.environmentd.sql(
        """
        DROP CLUSTER antiaffinity_cluster1 CASCADE;
        DROP CLUSTER antiaffinity_cluster2 CASCADE;
        DROP CLUSTER antiaffinity_cluster3 CASCADE;
        """
    )
