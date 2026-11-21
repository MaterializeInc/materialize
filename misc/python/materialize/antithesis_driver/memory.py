# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Log which pods hold the node's memory when little of it is left.

The platform's `Peak memory usage` fails once more than 95% of the VM's memory
is in use, and its samples name no process. The line logged shortly before
such a moment does. This only logs, and asserts nothing.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping
from typing import Any

from materialize.antithesis_driver.context import log

LOG_ABOVE = 0.85
INTERVAL_SECONDS = 5.0
TOP_PODS = 14
MIB = 1024 * 1024


def describe(summary: Mapping[str, Any]) -> tuple[float, str]:
    """The share of the node's memory in use, and the pods that hold most of it.

    `summary` is the kubelet's `/stats/summary`. Sizes are working sets, the
    memory the kernel cannot reclaim without swapping or killing. `outside
    pods` is what the node holds beyond its pods: the kubelet, the container
    runtime and the kernel.
    """
    node = summary["node"]["memory"]
    used = node["workingSetBytes"]
    total = used + node["availableBytes"]
    pods = sorted(
        (
            ((pod.get("memory") or {}).get("workingSetBytes", 0), pod["podRef"]["name"])
            for pod in summary.get("pods") or []
        ),
        reverse=True,
    )
    top = ", ".join(f"{name} {size // MIB}" for size, name in pods[:TOP_PODS])
    rest = sum(size for size, _ in pods[TOP_PODS:])
    outside = used - sum(size for size, _ in pods)
    return used / total, (
        f"memory: {used / total:.0%} of {total // MIB} MiB in use by {len(pods)} pods,"
        f" MiB per pod: {top}, other pods {rest // MIB}, outside pods {outside // MIB}"
    )


def start(core: Any) -> None:
    """Start logging from a thread of its own, so a slow kubelet stalls no rule."""
    threading.Thread(target=_loop, args=(core,), name="memory", daemon=True).start()


def _loop(core: Any) -> None:
    from materialize.orchestratord import K8S_TIMEOUT_SECONDS

    node: str | None = None
    failure: str | None = None
    while True:
        time.sleep(INTERVAL_SECONDS)
        try:
            if node is None:
                node = (
                    core.list_node(_request_timeout=K8S_TIMEOUT_SECONDS)
                    .items[0]
                    .metadata.name
                )
            response = core.connect_get_node_proxy_with_path(
                node,
                "stats/summary",
                _preload_content=False,
                _request_timeout=K8S_TIMEOUT_SECONDS,
            )
            share, line = describe(json.loads(response.data))
        except Exception as e:
            # Faults interrupt the request, and a starting kubelet reports no
            # memory yet. Neither is worth more than one line per cause.
            if type(e).__name__ != failure:
                failure = type(e).__name__
                log(f"memory: no reading: {failure}: {e}")
            continue
        failure = None
        if share >= LOG_ABOVE:
            log(line)
