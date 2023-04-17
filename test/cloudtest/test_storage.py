# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

import pytest
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.exists import exists, not_exists
from materialize.cloudtest.k8s import cluster_pod_name, cluster_service_name
from materialize.cloudtest.wait import wait


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


def test_source_creation(mz: MaterializeApplication) -> None:
    """Test that creating multiple sources causes multiple storage clusters to be spawned."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;

            > CREATE SOURCE source2
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;
            """
        )
    )

    for source in ["source1", "source2"]:
        [cluster_id, replica_id] = get_cluster_and_replica_id(mz, "mz_sources", source)

        storage_cluster = cluster_pod_name(cluster_id, replica_id)
        wait(condition="condition=Ready", resource=storage_cluster)

        mz.environmentd.sql(f"DROP SOURCE {source}")
        wait(condition="delete", resource=storage_cluster)


def test_source_resizing(mz: MaterializeApplication) -> None:
    """Test that resizing a given source causes the storage cluster to be replaced."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE SOURCE resize_source
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;
            """
        )
    )
    [cluster_id, replica_id] = get_cluster_and_replica_id(
        mz, "mz_sources", "resize_source"
    )
    storage_cluster_name = cluster_pod_name(cluster_id, replica_id)
    wait(condition="condition=Ready", resource=storage_cluster_name)

    assert get_cluster_workers(mz, cluster_id) == "'1'"
    assert get_cluster_scale(mz, cluster_id) == "'1'"
    assert get_cluster_size(mz, cluster_id) == "'1'"

    mz.testdrive.run(
        input=dedent(
            """
            > ALTER SOURCE resize_source
              SET (SIZE '16');
            """
        ),
        no_reset=True,
    )

    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/workers=16",
    )

    wait(condition="delete", resource=storage_cluster_name)

    assert get_cluster_workers(mz, cluster_id) == "'16'"
    assert get_cluster_scale(mz, cluster_id) == "'1'"
    assert get_cluster_size(mz, cluster_id) == "'16'"

    [cluster_id, replica_id] = get_cluster_and_replica_id(
        mz, "mz_sources", "resize_source"
    )
    storage_cluster_name = cluster_pod_name(cluster_id, replica_id)

    mz.environmentd.sql("DROP SOURCE resize_source")
    wait(condition="delete", resource=storage_cluster_name)


@pytest.mark.parametrize("failpoint", [False, True])
@pytest.mark.skip(reason="Failpoints mess up the Mz instance #18000")
def test_source_shutdown(mz: MaterializeApplication, failpoint: bool) -> None:
    print("Starting test_source_shutdown")
    if failpoint:
        mz.set_environmentd_failpoints("kubernetes_drop_service=return(error)")

    """Test that dropping a source causes its respective storage cluster to shut down."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

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
        print(f"Expected SQL error: {e}")

    if failpoint:
        # Disable failpoint here, this should end the crash loop of environmentd
        mz.set_environmentd_failpoints("")

    wait(condition="delete", resource=storage_cluster_pod)
    not_exists(storage_cluster_svc)


def test_sink_resizing(mz: MaterializeApplication) -> None:
    """Test that resizing a given sink causes the storage cluster to be replaced."""

    mz.testdrive.run(
        input=dedent(
            """
            > CREATE TABLE t1 (f1 int NOT NULL)

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${testdrive.schema-registry-url}'
              );

            > INSERT INTO t1 VALUES (1)

            > CREATE SINK resize_sink FROM t1
              INTO KAFKA CONNECTION kafka (TOPIC 'testdrive-sink-resize-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
              WITH (SIZE = '2')

            $ kafka-verify-data format=avro sink=materialize.public.resize_sink sort-messages=true
            {"before": null, "after": {"row":{"f1": 1}}}
            """
        )
    )
    assert id is not None

    [cluster_id, replica_id] = get_cluster_and_replica_id(mz, "mz_sinks", "resize_sink")
    storage_cluster_name = cluster_pod_name(cluster_id, replica_id)

    wait(condition="condition=Ready", resource=storage_cluster_name)

    assert get_cluster_workers(mz, cluster_id) == "'2'"
    assert get_cluster_scale(mz, cluster_id) == "'1'"
    assert get_cluster_size(mz, cluster_id) == "'2'"

    mz.testdrive.run(
        input=dedent(
            """
            > ALTER SINK resize_sink
              SET (SIZE '4-4');
            """
        ),
        no_reset=True,
    )

    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/workers=4",
    )

    wait(condition="delete", resource=storage_cluster_name)

    assert get_cluster_workers(mz, cluster_id) == "'4 4 4 4'"
    assert get_cluster_scale(mz, cluster_id) == "'4 4 4 4'"
    assert get_cluster_size(mz, cluster_id) == "'4-4 4-4 4-4 4-4'"

    [cluster_id, replica_id] = get_cluster_and_replica_id(mz, "mz_sinks", "resize_sink")
    storage_cluster_name = cluster_pod_name(cluster_id, replica_id)

    mz.environmentd.sql("DROP SINK resize_sink")
    wait(condition="delete", resource=storage_cluster_name)
