# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def test_computed_sizing(mz: MaterializeApplication) -> None:
    """Test that a SIZE N cluster indeed creates N computed instances."""
    SIZE = 2

    mz.environmentd.sql(
        f"CREATE CLUSTER sized1 REPLICAS (sized_replica1 (SIZE '{SIZE}-1'))"
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
        compute_pod = (
            f"pod/compute-cluster-{cluster_id}-replica-{replica_id}-{compute_id}"
        )
        wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")


def test_computed_shutdown(mz: MaterializeApplication) -> None:
    """Test that dropping a cluster or replica causes the associated computeds to shut down."""

    mz.environmentd.sql(
        "CREATE CLUSTER shutdown1 REPLICAS (shutdown_replica1 (SIZE '1'), shutdown_replica2 (SIZE '1'))"
    )

    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'shutdown1'"
    )[0][0]
    assert cluster_id is not None

    compute_pods = {}
    for replica_name in ["shutdown_replica1", "shutdown_replica2"]:
        replica_id = mz.environmentd.sql_query(
            f"SELECT id FROM mz_cluster_replicas WHERE name = '{replica_name}'"
        )[0][0]
        assert replica_id is not None

        compute_pod = f"pod/compute-cluster-{cluster_id}-replica-{replica_id}-0"
        compute_pods[replica_name] = compute_pod
        wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER REPLICA shutdown1.shutdown_replica1")
    wait(condition="delete", resource=compute_pods["shutdown_replica1"])

    mz.environmentd.sql("DROP CLUSTER shutdown1 CASCADE")
    wait(condition="delete", resource=compute_pods["shutdown_replica2"])
