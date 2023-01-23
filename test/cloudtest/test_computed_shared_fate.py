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

CLUSTER_SIZE = 8


def populate(mz: MaterializeApplication, seed: int) -> None:
    mz.testdrive.run(
        input=dedent(
            f"""
            > CREATE CLUSTER shared_fate REPLICAS (shared_fate_replica (SIZE '{CLUSTER_SIZE}-1'));
            > SET cluster = shared_fate;

            > CREATE TABLE t1 (f1 INTEGER);

            > INSERT INTO t1 SELECT 123000 + generate_series FROM generate_series(1, 1000);

            > CREATE DEFAULT INDEX ON t1;

            > INSERT INTO t1 SELECT 234000 + generate_series FROM generate_series(1, 1000);

            > CREATE CONNECTION kafka TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

            > CREATE SOURCE s1
              FROM KAFKA CONNECTION kafka (TOPIC 'testdrive-shared-fate-${{testdrive.seed}}')
              FORMAT BYTES
              ENVELOPE NONE;

            $ kafka-create-topic topic=shared-fate partitions={CLUSTER_SIZE}

            $ kafka-ingest format=bytes topic=shared-fate repeat=1000
            CDE${{kafka-ingest.iteration}}

            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1 UNION ALL SELECT COUNT(*) FROM s1;

            $ kafka-ingest format=bytes topic=shared-fate repeat=1000
            DEF${{kafka-ingest.iteration}}

            > CREATE DEFAULT INDEX ON v1;

            > SELECT COUNT(*) > 0 FROM s1;
            true
            """
        ),
        seed=seed,
    )


def validate(mz: MaterializeApplication, seed: int) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > SET cluster = shared_fate;

            > INSERT INTO t1 SELECT 345000 + generate_series FROM generate_series(1, 1000);

            $ kafka-ingest format=bytes topic=shared-fate repeat=1000
            EFG${kafka-ingest.iteration}

            > SELECT COUNT(*) FROM t1;
            3000

            > SELECT COUNT(*) FROM s1;
            3000

            > SELECT * FROM v1;
            3000
            3000

            > DROP CLUSTER shared_fate CASCADE;
            """
        ),
        no_reset=True,
        seed=seed,
    )


def kill_clusterd(
    mz: MaterializeApplication, compute_id: int, signal: str = "SIGKILL"
) -> None:
    cluster_id, replica_id = mz.environmentd.sql_query(
        f"SELECT cluster_id, id FROM mz_cluster_replicas WHERE name = 'shared_fate_replica'"
    )[0]

    pod_name = f"pod/cluster-{cluster_id}-replica-{replica_id}-{compute_id}"

    print(f"sending signal {signal} to pod {pod_name}...")

    try:
        mz.kubectl(
            "exec", pod_name, "--", "bash", "-c", f"kill -{signal} `pidof clusterd`"
        )
    except subprocess.CalledProcessError:
        # The clusterd process or container most likely has stopped already or is on its way
        pass


def test_kill_all_clusterds(mz: MaterializeApplication) -> None:
    """Kill all clusterds"""
    populate(mz, 1)

    for compute_id in range(0, CLUSTER_SIZE):
        kill_clusterd(mz, compute_id)

    validate(mz, 1)


def test_kill_one_clusterd(mz: MaterializeApplication) -> None:
    """Kill one clusterd out of $CLUSTER_SIZE"""
    populate(mz, 2)
    kill_clusterd(mz, round(CLUSTER_SIZE / 2))
    validate(mz, 2)


def test_kill_first_clusterd(mz: MaterializeApplication) -> None:
    """Kill the first clusterd out of $CLUSTER_SIZE"""
    populate(mz, 3)
    kill_clusterd(mz, 0)
    validate(mz, 3)


def test_kill_all_but_one_clusterd(mz: MaterializeApplication) -> None:
    """Kill all clusterds except one"""
    populate(mz, 4)
    for compute_id in list(range(0, 2)) + list(range(3, CLUSTER_SIZE)):
        kill_clusterd(mz, compute_id)

    validate(mz, 4)


def test_kill_while_suspended(mz: MaterializeApplication) -> None:
    """Suspend a clusterd and resume it after the rest of the cluster went down."""
    populate(mz, 5)
    kill_clusterd(mz, 2, signal="SIGSTOP")
    time.sleep(1)
    kill_clusterd(mz, 4)
    time.sleep(10)
    kill_clusterd(mz, 2, signal="SIGCONT")
    validate(mz, 5)


def test_suspend_while_killing(mz: MaterializeApplication) -> None:
    """Suspend a clusterd while the cluster is going down and resume it after."""
    populate(mz, 6)
    kill_clusterd(mz, 4)
    kill_clusterd(mz, 2, signal="SIGSTOP")
    time.sleep(10)
    kill_clusterd(mz, 2, signal="SIGCONT")
    validate(mz, 6)


def test_suspend_all_but_one(mz: MaterializeApplication) -> None:
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
