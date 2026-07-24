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

from materialize import MZ_ROOT, spawn
from materialize.cloudtest import DEFAULT_K8S_CONTEXT_NAME, DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.environmentd import MZ_INSTANCE_NAME
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
            MZ_INSTANCE_NAME,
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


def _profile_names(profiles_dir: str, kind: str) -> list[str]:
    """Basenames of the `<pod>.<kind>.pprof.gz` profiles mz-debug wrote, where
    `kind` is `cpuprof` or `memprof`."""
    return sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(profiles_dir, f"*.{kind}.pprof.gz"))
    )


def test_self_managed_profiles(mz: MaterializeApplication) -> None:
    """
    mz-debug must capture both a CPU and a heap profile from environmentd and
    from every clusterd pod, including each pod of a scaled (`scale > 1`)
    replica.

    A `scale=2` replica is a single Kubernetes service with two processes,
    but contains one pod per process.
    """
    SCALE = 2
    CLUSTER_NAME = "scaled_dbg"

    # Wait until the default cluster is ready, so environmentd is serving SQL.
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    # `REPLICATION FACTOR 1` pins the cluster to exactly one replica, so the
    # `scale=2` size yields exactly two clusterd pods behind one service.
    mz.environmentd.sql(
        f"CREATE CLUSTER {CLUSTER_NAME} SIZE 'scale={SCALE},workers=1', REPLICATION FACTOR 1"
    )
    rows = mz.environmentd.sql_query(
        "SELECT c.id, r.id "
        "FROM mz_cluster_replicas r "
        "JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name = '{CLUSTER_NAME}'"
    )
    assert len(rows) == 1, f"expected exactly one replica, got {rows}"
    cluster_id, replica_id = rows[0]

    # A scale=2 replica is served by one clusterd pod per process, ordinals
    # 0..SCALE, all behind a single service. `cluster_pod_name` returns the
    # `pod/...` resource string `kubectl wait` expects.
    pod_resources = [
        cluster_pod_name(cluster_id, replica_id, process) for process in range(SCALE)
    ]
    for pod_resource in pod_resources:
        wait(condition="condition=Ready", resource=pod_resource)

    print("-- Running mz-debug (CPU and heap profiles)")
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
            MZ_INSTANCE_NAME,
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
    # the run's `profiles/` directory. Assert both profile kinds are present for
    # environmentd and for every clusterd pod of the scaled replica. The scaled
    # replica's pods are matched by its unique cluster/replica id, which appears
    # in every one of its pod names.
    profiles_dir = os.path.join(_newest_dump_dir(), "profiles")
    replica_marker = f"cluster-{cluster_id}-replica-{replica_id}"

    for kind in ("cpuprof", "memprof"):
        names = _profile_names(profiles_dir, kind)
        print(f"{kind} profiles: {names}")

        assert f"{ENVIRONMENTD_POD}.{kind}.pprof.gz" in names, (
            f"mz-debug captured no {kind} profile for environmentd. "
            f"{kind} profiles: {names}"
        )

        replica_profiles = [name for name in names if replica_marker in name]
        assert len(replica_profiles) == SCALE, (
            f"mz-debug captured {len(replica_profiles)} {kind} profile(s) "
            f"({replica_profiles}) for the scale-{SCALE} replica, but it has "
            f"{SCALE} clusterd pods ({pod_resources}). Every pod behind the "
            "service must be profiled, with the pod ordinal in the filename. "
            f"All {kind} profiles: {names}"
        )

    mz.environmentd.sql(f"DROP CLUSTER {CLUSTER_NAME} CASCADE")
