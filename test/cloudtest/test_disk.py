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
from materialize.cloudtest.k8s import cluster_pod_name


def test_disk_replica(mz: MaterializeApplication) -> None:
    """Testing `DISK` cluster replicas"""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest key-format=bytes format=bytes topic=test
            key1:val1
            key2:val2

            > CREATE CLUSTER disk_cluster1
                REPLICAS (r1 (
                    SIZE '1', DISK = true
                ))

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE SOURCE source1
              IN CLUSTER disk_cluster1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              KEY FORMAT TEXT
              VALUE FORMAT TEXT
              ENVELOPE UPSERT;


            > SELECT * FROM source1;
            key           text
            ------------------
            key1          val1
            key2          val2

            $ kafka-ingest key-format=bytes format=bytes topic=test
            key1:val3

            > SELECT * FROM source1;
            key           text
            ------------------
            key1          val3
            key2          val2
            """
        )
    )

    cluster_id, replica_id = mz.environmentd.sql_query(
        "SELECT r.cluster_id, r.id as replica_id FROM mz_cluster_replicas r, mz_clusters c WHERE c.id = r.cluster_id AND c.name = 'disk_cluster1';"
    )[0]

    source_global_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_sources WHERE name = 'source1';"
    )[0][0]

    # verify that the replica's scratch directory contains data files for source1
    on_disk_sources = mz.kubectl(
        "exec",
        cluster_pod_name(cluster_id, replica_id),
        "-c",
        "clusterd",
        "--",
        "bash",
        "-c",
        "ls /scratch",
    )
    assert source_global_id in on_disk_sources
