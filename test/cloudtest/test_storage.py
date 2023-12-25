# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
from textwrap import dedent

import pytest
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.cluster import cluster_pod_name, cluster_service_name
from materialize.cloudtest.util.exists import exists, not_exists
from materialize.cloudtest.util.wait import wait

LOGGER = logging.getLogger(__name__)


def get_value_from_label(
    mz: MaterializeApplication, cluster_id: str, label: str
) -> str:
    return mz.kubectl(
        "get",
        "pods",
        f"--selector=cluster.environmentd.materialize.cloud/cluster-id={cluster_id}",
        "-o",
        "jsonpath='{.items[*].metadata.labels." + label + "}'",
    )


def get_cluster_workers(mz: MaterializeApplication, cluster_id: str) -> str:
    return get_value_from_label(
        mz, cluster_id, r"cluster\.environmentd\.materialize\.cloud/workers"
    )


def get_cluster_scale(mz: MaterializeApplication, cluster_id: str) -> str:
    return get_value_from_label(
        mz, cluster_id, r"cluster\.environmentd\.materialize\.cloud/scale"
    )


def get_cluster_size(mz: MaterializeApplication, cluster_id: str) -> str:
    return get_value_from_label(
        mz, cluster_id, r"cluster\.environmentd\.materialize\.cloud/size"
    )


def get_cluster_and_replica_id(
    mz: MaterializeApplication, mz_table: str, name: str
) -> tuple[str, str]:
    [cluster_id, replica_id] = mz.environmentd.sql_query(
        f"SELECT s.cluster_id, r.id FROM {mz_table} s JOIN mz_cluster_replicas r ON r.cluster_id = s.cluster_id WHERE s.name = '{name}'"
    )[0]
    return cluster_id, replica_id


@pytest.mark.parametrize("failpoint", [False, True])
@pytest.mark.skip(reason="Failpoints mess up the Mz instance #18000")
def test_source_shutdown(mz: MaterializeApplication, failpoint: bool) -> None:
    LOGGER.info("Starting test_source_shutdown")
    if failpoint:
        mz.set_environmentd_failpoints("kubernetes_drop_service=return(error)")

    """Test that dropping a source causes its respective storage cluster to shut down."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;

            # Those two objects do not currently create storage clusters
            # > CREATE MATERIALIZED VIEW view1 AS SELECT COUNT(*) FROM source1;

            # > CREATE SINK sink1
            #  FROM view1
            #  INTO KAFKA BROKER '${testdrive.kafka-addr}'
            #  TOPIC 'testdrive-sink1-${testdrive.seed}'
            #  FORMAT JSON;
            """
        )
    )

    [cluster_id, replica_id] = get_cluster_and_replica_id(mz, "mz_sources", "source1")

    storage_cluster_pod = cluster_pod_name(cluster_id, replica_id)
    storage_cluster_svc = cluster_service_name(cluster_id, replica_id)

    wait(condition="condition=Ready", resource=storage_cluster_pod)
    exists(storage_cluster_svc)

    mz.wait_for_sql()

    try:
        mz.environmentd.sql("DROP SOURCE source1")
    except InterfaceError as e:
        LOGGER.error(f"Expected SQL error: {e}")

    if failpoint:
        # Disable failpoint here, this should end the crash loop of environmentd
        mz.set_environmentd_failpoints("")

    wait(condition="delete", resource=storage_cluster_pod)
    not_exists(storage_cluster_svc)

