# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import urllib3

from materialize import orchestratord


def test_kubernetes_requests_have_timeouts() -> None:
    with patch.object(orchestratord, "client") as client:
        custom = client.CustomObjectsApi.return_value
        custom.get_namespaced_custom_object.return_value = {
            "status": {"resourceId": "r"}
        }
        apps = client.AppsV1Api.return_value
        apps.list_namespaced_stateful_set.return_value = SimpleNamespace(items=[])
        core = client.CoreV1Api.return_value
        core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
        mz = orchestratord.Materialize("ns", "env")
        mz.get()
        mz.create({})
        mz.patch_spec({})
        mz.environmentd_generations()
        mz.environmentd_pods()
        mz.clusterd_statefulset_generations()
        orchestratord.crd_established()
        calls = [
            *custom.method_calls,
            *apps.method_calls,
            *core.method_calls,
            *client.ApiextensionsV1Api.return_value.method_calls,
        ]
    assert calls
    assert all(0 < c.kwargs.get("_request_timeout", 0) <= 10 for c in calls), calls


def test_wait_until_retries_transport_timeouts() -> None:
    predicate = MagicMock(
        side_effect=[
            urllib3.exceptions.ReadTimeoutError(
                urllib3.HTTPConnectionPool("localhost"), None, "timeout"
            ),
            True,
        ]
    )
    with patch.object(orchestratord.time, "sleep"):
        orchestratord.wait_until(predicate, timeout=1)
    assert predicate.call_count == 2
