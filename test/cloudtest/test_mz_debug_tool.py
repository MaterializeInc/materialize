# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import glob
import os
import subprocess
import time

import pytest

from materialize import MZ_ROOT, spawn
from materialize.cloudtest import DEFAULT_K8S_CONTEXT_NAME, DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.cluster import cluster_pod_name
from materialize.cloudtest.util.wait import wait

# environmentd runs as a single-replica StatefulSet, so mz-debug profiles it as
# the pod `environmentd-0`.
ENVIRONMENTD_POD = "environmentd-0"


def test_successful_zip_creation(mz: MaterializeApplication) -> None:
    # Wait until the Materialize instance is ready
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    print("-- Port forwarding the internal SQL port")
    subprocess.Popen(
        [
            "kubectl",
            "--context",
            DEFAULT_K8S_CONTEXT_NAME,
            "port-forward",
            "pods/environmentd-0",
            "6877:6877",
        ]
    )

    print("-- Running mz-debug")
    spawn.runv(
        [
            "cargo",
            "run",
            "--bin",
            "mz-debug",
            "--",
            "self-managed",
            "--k8s-context",
            DEFAULT_K8S_CONTEXT_NAME,
            "--k8s-namespace",
            DEFAULT_K8S_NAMESPACE,
            "--mz-instance-name",
            mz.instance_identity.organization_name,
            "--mz-connection-url",
            "postgresql://mz_system@localhost:6877/materialize",
        ],
        cwd=MZ_ROOT,
    )

    print("-- Looking for mz-debug zip files")
    zip_files = glob.glob(str(MZ_ROOT / "mz_debug*.zip"))
    assert len(zip_files) > 0, "No mz-debug zip file was created"


def _newest_dump_dir() -> str:
    """The most recently written `mz_debug_<timestamp>` directory in MZ_ROOT,
    where mz-debug writes its output. Tests run sequentially against the shared
    (session-scoped) instance, so the newest directory belongs to the mz-debug
    run this test just made."""
    dump_dirs = [d for d in glob.glob(str(MZ_ROOT / "mz_debug_*")) if os.path.isdir(d)]
    assert dump_dirs, "mz-debug did not create an mz_debug_* output directory"
    return max(dump_dirs, key=os.path.getmtime)


def _profile_names(profiles_dir: str, kind: str, written_after: float) -> list[str]:
    """Basenames of the `<pod>.<kind>.pprof.gz` profiles written after
    `written_after`, a `time.time()` timestamp taken before the mz-debug run
    under test. `kind` is `cpuprof` or `memprof`.

    mz-debug names its output directory after the current minute, so runs a few
    seconds apart share one and a run overwrites what an earlier run captured for
    the same pod. Ignoring profiles older than the run keeps a failure to capture
    from being masked by an earlier run's leftovers."""
    return sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(profiles_dir, f"*.{kind}.pprof.gz"))
        if os.path.getmtime(p) >= written_after
    )


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
    mz-debug must capture both a CPU and a heap profile from environmentd and
    from every clusterd pod of a cluster.

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

    print("-- Running mz-debug (CPU and heap profiles)")
    # Filesystems can store modification times at a coarser resolution than
    # `time.time()` reports, so leave a second of slack for a profile written
    # right after the run starts.
    run_started = time.time() - 1
    # Capture only profiles to keep the run focused.
    spawn.runv(
        [
            "cargo",
            "run",
            "--bin",
            "mz-debug",
            "--",
            "self-managed",
            "--k8s-context",
            DEFAULT_K8S_CONTEXT_NAME,
            "--k8s-namespace",
            DEFAULT_K8S_NAMESPACE,
            "--mz-instance-name",
            mz.instance_identity.organization_name,
            "--mz-connection-url",
            "postgresql://mz_system@localhost:6877/materialize",
            "--dump-k8s=false",
            "--dump-system-catalog=false",
            "--dump-prometheus-metrics=false",
            "--dump-heap-profiles=true",
            "--dump-cpu-profiles=true",
            "--cpu-profile-duration-seconds=1",
        ],
        cwd=MZ_ROOT,
    )

    # mz-debug writes `<pod>.cpuprof.pprof.gz` and `<pod>.memprof.pprof.gz` under
    # the run's `profiles/` directory. Both kinds must be there for environmentd
    # and for every clusterd pod of every replica, each named after the pod it
    # came from.
    profiles_dir = os.path.join(_newest_dump_dir(), "profiles")
    expected_pods = [ENVIRONMENTD_POD] + [
        pod_resource.removeprefix("pod/") for pod_resource in pod_resources
    ]

    for kind in ("cpuprof", "memprof"):
        names = _profile_names(profiles_dir, kind, run_started)
        print(f"{kind} profiles: {names}")

        missing = [
            pod for pod in expected_pods if f"{pod}.{kind}.pprof.gz" not in names
        ]
        assert not missing, (
            f"mz-debug captured no {kind} profile for {missing}. Every pod of "
            f"every replica must be profiled, under a name that identifies the "
            f"pod. {kind} profiles: {names}"
        )

    mz.environmentd.sql(f"DROP CLUSTER {cluster_name} CASCADE")
