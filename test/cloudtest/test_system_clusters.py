# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json

from materialize.cloudtest.app.materialize_application import MaterializeApplication


def assert_pod_properties(mz: MaterializeApplication, pod_name: str) -> None:
    pod_desc = json.loads(mz.kubectl("get", "-o", "json", "pod", pod_name))
    node_selector = pod_desc["spec"]["nodeSelector"]

    expected_disk_selection_mode = "true"

    assert any(
        "disk" in k and v == expected_disk_selection_mode
        for k, v in node_selector.items()
    ), f"pod {pod_name} does not have the disk={expected_disk_selection_mode} selector"
    assert not any(
        "availability-zone" in k for k in node_selector.keys()
    ), f"pod {pod_name} has a availability-zone selector"


def test_system_clusters(mz: MaterializeApplication) -> None:
    """Confirm that the system clusters have the expected labels and selectors"""
    mz.wait_replicas()

    assert_pod_properties(mz, "cluster-s1-replica-s1-gen-0-0")
    assert_pod_properties(mz, "cluster-s2-replica-s2-gen-0-0")
