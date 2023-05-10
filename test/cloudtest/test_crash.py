# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import Tuple

from kubernetes.client import V1Pod, V1StatefulSet
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s import cluster_pod_name
from materialize.cloudtest.wait import wait


def populate(mz: MaterializeApplication, seed: int) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > CREATE TABLE t1 (f1 INTEGER);

            > INSERT INTO t1 VALUES (123);

            > CREATE DEFAULT INDEX ON t1;

            > INSERT INTO t1 VALUES (234);

            > CREATE CONNECTION kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE SOURCE s1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-crash-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;

            $ kafka-create-topic topic=crash

            $ kafka-ingest format=bytes topic=crash
            CDE

            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1 UNION ALL SELECT COUNT(*) FROM s1;

            $ kafka-ingest format=bytes topic=crash
            DEF

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
            > INSERT INTO t1 VALUES (345);

            $ kafka-ingest format=bytes topic=crash
            EFG

            > SELECT COUNT(*) FROM t1;
            3

            > SELECT COUNT(*) FROM s1;
            3

            > SELECT * FROM v1;
            3
            3
            """
        ),
        no_reset=True,
        seed=seed,
    )


def test_crash_storage(mz: MaterializeApplication) -> None:
    populate(mz, 1)

    [cluster_id, replica_id] = mz.environmentd.sql_query(
        "SELECT s.cluster_id, r.id FROM mz_sources s JOIN mz_cluster_replicas r ON r.cluster_id = s.cluster_id WHERE s.name = 's1'"
    )[0]
    pod_name = cluster_pod_name(cluster_id, replica_id)

    wait(condition="jsonpath={.status.phase}=Running", resource=pod_name)
    mz.kubectl("exec", pod_name, "--", "bash", "-c", "kill -9 `pidof clusterd` || true")
    wait(condition="jsonpath={.status.phase}=Running", resource=pod_name)

    validate(mz, 1)


def test_crash_environmentd(mz: MaterializeApplication) -> None:
    def restarts(p: V1Pod) -> int:
        assert p.status is not None
        assert p.status.container_statuses is not None
        return p.status.container_statuses[0].restart_count

    def get_replica() -> Tuple[V1Pod, V1StatefulSet]:
        """Find the stateful set for the replica of the default cluster"""
        compute_pod_name = "cluster-u1-replica-1-0"
        ss_name = "cluster-u1-replica-1"
        compute_pod = mz.environmentd.api().read_namespaced_pod(
            compute_pod_name, mz.environmentd.namespace()
        )
        for ss in (
            mz.environmentd.apps_api().list_stateful_set_for_all_namespaces().items
        ):
            assert ss.metadata is not None
            if ss.metadata.name == ss_name:
                return (compute_pod, ss)
        assert False

    populate(mz, 2)

    before = get_replica()

    try:
        mz.environmentd.sql("SELECT mz_internal.mz_panic('forced panic')")
    except InterfaceError:
        pass
    validate(mz, 2)

    after = get_replica()

    # A environmentd crash must not restart other nodes
    assert restarts(before[0]) == restarts(after[0])


def test_crash_clusterd(mz: MaterializeApplication) -> None:
    populate(mz, 3)
    mz.testdrive.run(
        input=dedent(
            """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_unstable_dependencies = true;
            """
        ),
        no_reset=True,
    )
    mz.environmentd.sql("CREATE TABLE crash_table (f1 TEXT)")
    mz.environmentd.sql(
        "CREATE MATERIALIZED VIEW crash_view AS SELECT mz_internal.mz_panic(f1) FROM crash_table"
    )
    mz.environmentd.sql("INSERT INTO crash_table VALUES ('forced panic')")

    mz.testdrive.run(
        input=dedent(
            """
            > DROP MATERIALIZED VIEW crash_view
            """
        ),
        no_reset=True,
        seed=3,
    )

    validate(mz, 3)
