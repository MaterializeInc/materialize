# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def zones_used(mz: MaterializeApplication) -> int:
    replica_names = [
        "antiaffinity_replica1",
        "antiaffinity_replica2",
        "antiaffinity_replica3",
    ]
    nodes = {}

    for replica_name in replica_names:
        cluster_id, replica_id = mz.environmentd.sql_query(
            f"SELECT cluster_id, id FROM mz_cluster_replicas WHERE name = '{replica_name}'"
        )[0]
        assert replica_id is not None

        compute_pod_name = f"compute-cluster-{cluster_id}-replica-{replica_id}-0"

        wait(condition="condition=Ready", resource=f"pod/{compute_pod_name}")

        compute_pod = mz.environmentd.api().read_namespaced_pod(
            compute_pod_name, mz.environmentd.namespace()
        )
        spec = compute_pod.spec
        assert spec is not None

        nodes[spec.node_name] = 1

    return len(nodes.keys())


def test_create_cluster_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple replicas as defined in CREATE CLUSTER are placed in different availability zones."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS (
            antiaffinity_replica1 (SIZE '1'),
            antiaffinity_replica2 (SIZE '1'),
            antiaffinity_replica3 (SIZE '1')
        )"""
    )

    assert zones_used(mz) == 3

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


def test_create_cluster_replica_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple replicas as created with CREATE CLUSTER REPLICA are placed in different availability zones."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS ();
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica1 SIZE '1';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica2 SIZE '1';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica3 SIZE '1';
        """
    )

    assert zones_used(mz) == 3

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


@pytest.mark.skip(reason="https://github.com/MaterializeInc/materialize/issues/14170")
def test_create_cluster_replica_zone_specified(mz: MaterializeApplication) -> None:
    """Test that the AVAILABILITY ZONE argument to CREATE CLUSTER REPLICA is observed."""
    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS ();
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica1 SIZE '1' , AVAILABILITY ZONE 'kind-worker3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica2 SIZE '1' , AVAILABILITY ZONE 'kind-worker3';
        CREATE CLUSTER REPLICA antiaffinity_cluster1.antiaffinity_replica3 SIZE '1' , AVAILABILITY ZONE 'kind-worker3';
        """
    )

    assert zones_used(mz) == 1

    mz.environmentd.sql("DROP CLUSTER antiaffinity_cluster1 CASCADE")


@pytest.mark.skip(reason="Not currently guaranteed by implementation.")
def test_create_clusters_antiaffinity(mz: MaterializeApplication) -> None:
    """Test that multiple independent clusters are spread out to different availability zones."""

    mz.environmentd.sql(
        """
        CREATE CLUSTER antiaffinity_cluster1 REPLICAS (
            antiaffinity_replica1 (SIZE '1')
        );
        CREATE CLUSTER antiaffinity_cluster2 REPLICAS (
            antiaffinity_replica2 (SIZE '1')
        );
        CREATE CLUSTER antiaffinity_cluster3 REPLICAS (
            antiaffinity_replica3 (SIZE '1')
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
