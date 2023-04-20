# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
import time
from textwrap import dedent

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s import cluster_pod_name

CLUSTER_SIZE = 8
NUM_SOURCES = 4


def populate(mz: MaterializeApplication, seed: int) -> None:
    create_sources_sinks = "\n".join(
        f"""
            > CREATE SOURCE source{i}
              IN CLUSTER storage_shared_fate
              FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-storage-shared-fate-${{testdrive.seed}}')
              FORMAT BYTES
              ENVELOPE NONE;

            > CREATE MATERIALIZED VIEW v{i} AS SELECT COUNT(*) FROM source{i};

            > CREATE TABLE t{i} (f1 INTEGER);

            > INSERT INTO t{i} SELECT 123000 + generate_series FROM generate_series(1, 1000);

            $ kafka-create-topic topic=storage-shared-fate-sink{i} partitions={CLUSTER_SIZE*4}

            > CREATE SINK sink{i}
              IN CLUSTER storage_shared_fate FROM t{i}
              INTO KAFKA CONNECTION kafka (TOPIC 'testdrive-storage-shared-fate-sink{i}-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM;

            > CREATE SOURCE sink{i}_check
              IN CLUSTER storage_shared_fate
              FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-storage-shared-fate-sink{i}-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE;
    """
        for i in range(NUM_SOURCES)
    )

    check_counts = "\n".join(
        f"""
            > SELECT COUNT(*) FROM source{i};
            2000

            > SELECT COUNT(*) FROM sink{i}_check;
            1000
    """
        for i in range(NUM_SOURCES)
    )

    mz.testdrive.run(
        input=dedent(
            f"""
            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${{testdrive.schema-registry-url}}'
              );

            > CREATE CLUSTER storage_shared_fate REPLICAS (storage_shared_fate_replica (SIZE '{CLUSTER_SIZE}-1'));

            > CREATE CONNECTION kafka TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

            $ kafka-create-topic topic=storage-shared-fate partitions={CLUSTER_SIZE*4}

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=storage-shared-fate repeat=1000
            CDE${{kafka-ingest.iteration}}:CDE${{kafka-ingest.iteration}}

            {create_sources_sinks}

            $ kafka-ingest key-format=bytes format=bytes key-terminator=: topic=storage-shared-fate repeat=1000
            DEF${{kafka-ingest.iteration}}:DEF${{kafka-ingest.iteration}}

            {check_counts}
            """
        ),
        seed=seed,
    )


def validate(mz: MaterializeApplication, seed: int) -> None:
    validations = "\n".join(
        f"""
            > INSERT INTO t{i} SELECT 234000 + generate_series FROM generate_series(1, 1000);

            > SELECT COUNT(*) FROM source{i};
            3000

            > SELECT * FROM v{i};
            3000

            > SELECT COUNT(*) FROM sink{i}_check;
            2000
    """
        for i in range(NUM_SOURCES)
    )

    mz.testdrive.run(
        input=dedent(
            f"""
            $ kafka-ingest key-format=bytes format=bytes key-terminator=: topic=storage-shared-fate repeat=1000
            EFG${{kafka-ingest.iteration}}:EFG${{kafka-ingest.iteration}}

            {validations}

            > DROP CLUSTER storage_shared_fate CASCADE;
            """
        ),
        no_reset=True,
        seed=seed,
    )


def kill_clusterd(
    mz: MaterializeApplication, compute_id: int, signal: str = "SIGKILL"
) -> None:
    cluster_id, replica_id = mz.environmentd.sql_query(
        "SELECT cluster_id, id FROM mz_cluster_replicas WHERE name = 'storage_shared_fate_replica'"
    )[0]

    pod_name = cluster_pod_name(cluster_id, replica_id, compute_id)

    print(f"sending signal {signal} to pod {pod_name}...")

    try:
        mz.kubectl(
            "exec", pod_name, "--", "bash", "-c", f"kill -{signal} `pidof clusterd`"
        )
    except subprocess.CalledProcessError:
        # The clusterd process or container most likely has stopped already or is on its way
        pass


def test_kill_all_storage_clusterds(mz: MaterializeApplication) -> None:
    """Kill all clusterds"""
    populate(mz, 1)

    for compute_id in range(0, CLUSTER_SIZE):
        kill_clusterd(mz, compute_id)

    validate(mz, 1)


def test_kill_one_storage_clusterd(mz: MaterializeApplication) -> None:
    """Kill one clusterd out of $CLUSTER_SIZE"""
    populate(mz, 2)
    kill_clusterd(mz, round(CLUSTER_SIZE / 2))
    validate(mz, 2)


def test_kill_first_storage_clusterd(mz: MaterializeApplication) -> None:
    """Kill the first clusterd out of $CLUSTER_SIZE"""
    populate(mz, 3)
    kill_clusterd(mz, 0)
    validate(mz, 3)


def test_kill_all_but_one_storage_clusterd(mz: MaterializeApplication) -> None:
    """Kill all clusterds except one"""
    populate(mz, 4)
    for compute_id in list(range(0, 2)) + list(range(3, CLUSTER_SIZE)):
        kill_clusterd(mz, compute_id)

    validate(mz, 4)


def test_kill_storage_while_suspended(mz: MaterializeApplication) -> None:
    """Suspend a clusterd and resume it after the rest of the cluster went down."""
    populate(mz, 5)
    kill_clusterd(mz, 2, signal="SIGSTOP")
    time.sleep(1)
    kill_clusterd(mz, 4)
    time.sleep(10)
    kill_clusterd(mz, 2, signal="SIGCONT")
    validate(mz, 5)


def test_suspend_while_killing_storage(mz: MaterializeApplication) -> None:
    """Suspend a clusterd while the cluster is going down and resume it after."""
    populate(mz, 6)
    kill_clusterd(mz, 4)
    kill_clusterd(mz, 2, signal="SIGSTOP")
    time.sleep(10)
    kill_clusterd(mz, 2, signal="SIGCONT")
    validate(mz, 6)


def test_suspend_all_but_one_storage(mz: MaterializeApplication) -> None:
    """Suspend all clusterds while killing one."""
    populate(mz, 7)

    for compute_id in range(0, CLUSTER_SIZE):
        if compute_id != 4:
            kill_clusterd(mz, compute_id, signal="SIGSTOP")

    kill_clusterd(mz, 4)
    time.sleep(10)

    for compute_id in range(0, CLUSTER_SIZE):
        if compute_id != 4:
            kill_clusterd(mz, compute_id, signal="SIGCONT")

    validate(mz, 7)
