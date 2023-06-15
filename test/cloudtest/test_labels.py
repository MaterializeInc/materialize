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


# NOTE [btv] - Quick and dirty hack: assume s1 is always mz_system and
# s2 is always mz_introspection.
#
# This will need to be done properly (i.e., by actually looking up
# the cluster id->name mapping in SQL) if that assumption ever changes.
def test_roles(mz: MaterializeApplication) -> None:
    mz.wait_replicas()
    pods = json.loads(mz.kubectl("get", "pods", "-o", "json"))
    names_roles = (
        (
            item["metadata"]["name"],
            item["metadata"]
            .get("labels", {})
            .get("cluster.environmentd.materialize.cloud/replica-role"),
        )
        for item in pods["items"]
    )
    n_replica_pods = 0
    for (name, role) in names_roles:
        if name.startswith("cluster-s1"):
            assert role == "system-critical"
            n_replica_pods += 1
        elif name.startswith("cluster-s2"):
            assert role == "system"
            n_replica_pods += 1
        elif name.startswith("cluster-u"):
            assert role == "user"
            n_replica_pods += 1
    assert n_replica_pods >= 3
