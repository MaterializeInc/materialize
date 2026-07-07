# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from materialize.cloudtest.app.application import Application


def cluster_pod_name(cluster_id: str, replica_id: str, process: int = 0) -> str:
    return f"pod/cluster-{cluster_id}-replica-{replica_id}-gen-0-{process}"


def cluster_service_name(cluster_id: str, replica_id: str) -> str:
    return f"service/cluster-{cluster_id}-replica-{replica_id}-gen-0"


def signal_process_in_pod(
    app: Application, pod_name: str, process: str, signal: str
) -> None:
    """Send a signal to a process running inside a pod's container.

    Distroless images ship no shell, so signals cannot be sent via
    `kubectl exec`. Instead, exec into the kind node (a Docker container on
    the host) that runs the pod and signal the process from there. Unlike a
    pod delete, this hits the process in place: the pod object survives, the
    container restarts through the kubelet (for fatal signals), and
    SIGSTOP/SIGCONT suspend and resume the process without Kubernetes
    noticing.

    Raises `subprocess.CalledProcessError` if the pod, container, or process
    cannot be found, e.g. because it is already gone.
    """
    node = app.kubectl("get", pod_name, "-o", "jsonpath={.spec.nodeName}").strip()
    name = pod_name.removeprefix("pod/")
    # POSIX `kill` wants signal names without the SIG prefix.
    sig = signal.removeprefix("SIG")
    # The target process is either the container's init or a direct child of
    # it: distroless images run tini as init with the target as its child,
    # while Ubuntu-based images `exec` the target from their entrypoint.
    script = f"""
        set -eu
        pod_id=$(crictl pods -q --name '{name}' --state ready | head -n1)
        cid=$(crictl ps -q --pod "$pod_id" | head -n1)
        init=$(crictl inspect -o go-template --template '{{{{.info.pid}}}}' "$cid")
        for pid in $init $(cat "/proc/$init/task/$init/children"); do
            if [ "$(cat /proc/$pid/comm)" = '{process}' ]; then
                kill -s {sig} "$pid"
                exit 0
            fi
        done
        echo "no {process} process found in pod {name}" >&2
        exit 1
    """
    subprocess.run(
        ["docker", "exec", node, "sh", "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
