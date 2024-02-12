# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.cluster import cluster_pod_name
from materialize.cloudtest.util.wait import wait


def test_managed_cluster_sizing(mz: MaterializeApplication) -> None:
    """Test that a SIZE N cluster indeed creates N clusterd instances."""
    SIZE = 2

    mz.environmentd.sql(f"CREATE CLUSTER sized1 SIZE '{SIZE}-1', REPLICATION FACTOR 2")
    cluster_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert cluster_id is not None

    check = mz.environmentd.sql_query(
        "SELECT availability_zones IS NULL FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert check is not None
    assert check == True

    mz.environmentd.sql("ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('1', '2', '3'))")
    check = mz.environmentd.sql_query(
        "SELECT list_length(availability_zones) = 3 FROM mz_clusters WHERE name = 'sized1'"
    )[0][0]
    assert check is not None
    assert check == True

    mz.testdrive.run(
        input=dedent(
            """
            ! ALTER CLUSTER sized1 SET (AVAILABILITY ZONES ('4'))
            exact:unknown cluster replica availability zone 4
            """
        ),
        no_reset=True,
    )

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert [replica[0] for replica in replicas] == ["r1", "r2"]

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("ALTER CLUSTER sized1 SET (REPLICATION FACTOR 1)")

    replicas = mz.environmentd.sql_query(
        "SELECT mz_cluster_replicas.name, mz_cluster_replicas.id FROM mz_cluster_replicas JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id WHERE mz_clusters.name = 'sized1' ORDER BY 1"
    )
    assert [replica[0] for replica in replicas] == ["r1"]

    for compute_id in range(0, SIZE):
        for replica in replicas:
            compute_pod = cluster_pod_name(cluster_id, replica[1], compute_id)
            wait(condition="condition=Ready", resource=compute_pod)

    mz.environmentd.sql("DROP CLUSTER sized1 CASCADE")
