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


def test_replica_metrics(mz: MaterializeApplication) -> None:
    mz.testdrive.run(
        input=dedent("""
            > CREATE CLUSTER my_cluster REPLICAS (my_replica (SIZE 'scale=4,workers=4'))

            > SELECT process_id
              FROM mz_internal.mz_cluster_replica_metrics m
              JOIN mz_cluster_replicas cr ON m.replica_id = cr.id
              WHERE
                  cr.name = 'my_replica' AND
                  m.cpu_nano_cores IS NOT NULL AND
                  m.memory_bytes IS NOT NULL
            0
            1
            2
            3

            > SELECT DISTINCT process_id
              FROM mz_internal.mz_cluster_replica_metrics_history m
              JOIN mz_cluster_replicas cr ON m.replica_id = cr.id
              WHERE
                  cr.name = 'my_replica' AND
                  m.cpu_nano_cores IS NOT NULL AND
                  m.memory_bytes IS NOT NULL
            0
            1
            2
            3

            > DROP CLUSTER my_cluster
            """),
    )


def test_prometheus_sql_metrics(mz: MaterializeApplication) -> None:
    # We need a source to have any metrics under `metrics/mz_storage
    mz.testdrive.run(
        input=dedent("""
            > CREATE SOURCE counter FROM LOAD GENERATOR COUNTER
            """),
    )

    def check_metrics(metric_group_name: str, metric_name: str):
        metrics = mz.environmentd.http_get(f"/metrics/{metric_group_name}")

        found = False
        for metric in metrics.splitlines():
            if metric.startswith(metric_name):
                found = True

        assert found, "could not read metrics"

    check_metrics("mz_frontier", "mz_read_frontier")
    check_metrics("mz_usage", "mz_clusters_count")
    check_metrics("mz_compute", "mz_compute_replica_park_duration_seconds_total")
    check_metrics("mz_storage", "mz_storage_objects")


def test_public_metrics_endpoint(mz: MaterializeApplication) -> None:
    """Verify that `/metrics/public` federates clusterd replica scrapes
    and stamps `cluster_name` labels onto replica-sourced metrics, and that
    those labels disappear once the cluster is dropped."""

    cluster_name = "test_cluster_1"
    label = f'cluster_name="{cluster_name}"'

    # Create a cluster replica and wait for the cluster to be online. We use metrics to check if the cluster is online.
    mz.testdrive.run(
        input=dedent(
            f"""
            > CREATE CLUSTER {cluster_name} REPLICAS (r1 (SIZE 'scale=2,workers=2'))

            > SELECT process_id
              FROM mz_internal.mz_cluster_replica_metrics m
              JOIN mz_cluster_replicas cr ON m.replica_id = cr.id
              JOIN mz_clusters c ON cr.cluster_id = c.id
              WHERE
                  c.name = '{cluster_name}' AND
                  m.cpu_nano_cores IS NOT NULL AND
                  m.memory_bytes IS NOT NULL
            0
            1
            """,
        ),
        no_reset=True,
    )

    body = mz.environmentd.http_get("metrics/public")
    if label not in body:
        raise AssertionError(
            f"Expected {label} in /metrics/public after CREATE CLUSTER."
        )

    mz.testdrive.run(
        input=dedent(
            f"""
            > DROP CLUSTER {cluster_name}
            """,
        ),
        no_reset=True,
    )

    body = mz.environmentd.http_get("metrics/public")
    if label in body:
        raise AssertionError(
            f"{label} still appears in /metrics/public after DROP CLUSTER."
        )
