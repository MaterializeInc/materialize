# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests of the mz-debug collector against a cloudtest instance.

cloudtest runs the collector directly (see
`materialize.cloudtest.k8s.debug_collector`) since it has no operator, so
these tests drive the collector's HTTP API the way `mz-debug self-managed`
does: request a snapshot, wait for it, download the zip, inspect it.
"""

import io
import time
import zipfile
from typing import Any

import pytest
import requests

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.cluster import cluster_pod_name
from materialize.cloudtest.util.wait import wait

# environmentd runs as a single-replica StatefulSet, so the collector profiles
# it as the pod `environmentd-0`.
ENVIRONMENTD_POD = "environmentd-0"

# A snapshot of every category on a small instance takes a minute or two; the
# system catalog dump dominates.
SNAPSHOT_TIMEOUT_SECS = 600


def _take_snapshot(
    mz: MaterializeApplication, request: dict[str, Any]
) -> zipfile.ZipFile:
    """Requests an on-demand snapshot with the given category overrides, waits
    for the collector to complete it, and returns the downloaded zip."""
    base_url = mz.debug_collector.base_url()
    wait(condition="condition=Available", resource="deployment/debug-collector")

    response = requests.post(f"{base_url}/api/snapshots", json=request, timeout=30)
    response.raise_for_status()
    snapshot_id = response.json()["id"]
    print(f"-- Requested snapshot {snapshot_id}")

    deadline = time.time() + SNAPSHOT_TIMEOUT_SECS
    while True:
        listing = requests.get(f"{base_url}/api/snapshots", timeout=30)
        listing.raise_for_status()
        body = listing.json()
        if any(meta["id"] == snapshot_id for meta in body["snapshots"]):
            break
        queued = [
            status["id"]
            for status in (body["in_progress"], body["pending"])
            if status is not None
        ]
        assert snapshot_id in queued, (
            f"snapshot {snapshot_id} is neither complete nor queued; "
            f"last error: {body['last_error']}"
        )
        assert time.time() < deadline, f"snapshot {snapshot_id} did not complete"
        time.sleep(2)

    download = requests.get(f"{base_url}/api/snapshots/{snapshot_id}", timeout=300)
    download.raise_for_status()
    assert download.headers["x-mz-debug-snapshot-id"] == snapshot_id
    archive = zipfile.ZipFile(io.BytesIO(download.content))
    root = f"mz_debug_{snapshot_id}/"
    assert all(
        name.startswith(root) for name in archive.namelist()
    ), f"every entry must sit under {root}: {archive.namelist()[:10]}"
    return archive


def _entries(archive: zipfile.ZipFile, directory: str) -> list[str]:
    """The entries under `directory` (relative to the snapshot root), with the
    root and directory stripped."""
    prefix = f"/{directory}/"
    return sorted(
        name.split(prefix, 1)[1] for name in archive.namelist() if prefix in name
    )


def test_snapshot_contains_every_category(mz: MaterializeApplication) -> None:
    # Wait until the Materialize instance is ready
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    # CPU profiles are exercised by the profiling test; leave them out here so
    # the full-category snapshot stays quick.
    archive = _take_snapshot(mz, {"cpu_profiles": False})
    names = archive.namelist()
    print(f"snapshot entries: {len(names)}")

    assert any(name.endswith("/snapshot.json") for name in names), names[:20]
    pods = _entries(archive, "pods/materialize")
    assert f"{ENVIRONMENTD_POD}.yaml" in pods, pods
    assert "describe.txt" in pods, pods
    logs = _entries(archive, "logs/materialize")
    assert f"{ENVIRONMENTD_POD}.current.log" in logs, logs
    metrics = _entries(archive, "prom_metrics")
    assert f"{ENVIRONMENTD_POD}.metrics.txt" in metrics, metrics
    profiles = _entries(archive, "profiles")
    assert f"{ENVIRONMENTD_POD}.memprof.pprof.gz" in profiles, profiles
    assert not any(name.endswith(".cpuprof.pprof.gz") for name in profiles), profiles
    catalog = _entries(archive, "system_catalog")
    assert "mz_clusters.csv" in catalog, catalog


@pytest.mark.parametrize(
    "scale,replication_factor",
    [
        # A single service fronting one pod per process.
        (2, 1),
        # One service per replica, each fronting a single pod.
        (1, 2),
    ],
)
def test_self_managed_profiles(
    mz: MaterializeApplication, scale: int, replication_factor: int
) -> None:
    """
    The collector must capture both a CPU and a heap profile from environmentd
    and from every clusterd pod of a cluster.

    A `scale=N` replica is a single Kubernetes service with N processes, but
    contains one pod per process. A cluster of replication factor N is N such
    services. Both dimensions have to be walked to reach every pod.
    """
    cluster_name = f"dbg_scale{scale}_rf{replication_factor}"

    # Wait until the default cluster is ready, so environmentd is serving SQL.
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    mz.environmentd.sql(
        f"CREATE CLUSTER {cluster_name} SIZE 'scale={scale},workers=1', "
        f"REPLICATION FACTOR {replication_factor}"
    )
    rows = mz.environmentd.sql_query(
        "SELECT c.id, r.id "
        "FROM mz_cluster_replicas r "
        "JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name = '{cluster_name}'"
    )
    assert (
        len(rows) == replication_factor
    ), f"expected {replication_factor} replica(s), got {rows}"
    cluster_id = rows[0][0]
    replica_ids = [replica_id for _, replica_id in rows]

    # Each replica is served by one clusterd pod per process, ordinals 0..scale,
    # all behind a single service. `cluster_pod_name` returns the `pod/...`
    # resource string `kubectl wait` expects.
    pod_resources = [
        cluster_pod_name(cluster_id, replica_id, process)
        for replica_id in replica_ids
        for process in range(scale)
    ]
    for pod_resource in pod_resources:
        wait(condition="condition=Ready", resource=pod_resource)

    print("-- Taking a snapshot with CPU and heap profiles")
    # Capture only profiles to keep the snapshot focused.
    archive = _take_snapshot(
        mz,
        {
            "k8s": False,
            "system_catalog": False,
            "prometheus_metrics": False,
            "heap_profiles": True,
            "cpu_profiles": True,
            "cpu_profile_duration_seconds": 1,
        },
    )

    # The collector writes `<pod>.cpuprof.pprof.gz` and `<pod>.memprof.pprof.gz`
    # under the snapshot's `profiles/` directory. Both kinds must be there for
    # environmentd and for every clusterd pod of every replica, each named
    # after the pod it came from.
    profiles = _entries(archive, "profiles")
    expected_pods = [ENVIRONMENTD_POD] + [
        pod_resource.removeprefix("pod/") for pod_resource in pod_resources
    ]
    for kind in ("cpuprof", "memprof"):
        missing = [
            pod for pod in expected_pods if f"{pod}.{kind}.pprof.gz" not in profiles
        ]
        assert not missing, (
            f"the collector captured no {kind} profile for {missing}. Every pod of "
            f"every replica must be profiled, under a name that identifies the "
            f"pod. profiles: {profiles}"
        )

    mz.environmentd.sql(f"DROP CLUSTER {cluster_name} CASCADE")
