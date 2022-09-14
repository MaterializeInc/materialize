# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s.minio import mc_command
from materialize.cloudtest.wait import wait


def test_usage_snapshots(mz: MaterializeApplication) -> None:
    mz.environmentd.sql(f"CREATE CLUSTER c REPLICAS (cr1 (SIZE '2'))")
    time.sleep(2)
    audit_rows = mz.environmentd.sql_query(
        "SELECT id, event_type, object_type, event_details, user FROM mz_audit_events"
    )
    # event_type, object_type, event_details, user
    assert audit_rows[0][1:] == ["create", "cluster", {"name": "c"}, "materialize"]
    assert audit_rows[1][1:] == [
        "create",
        "cluster-replica",
        {"cluster_name": "c", "logical_size": "2", "replica_name": "cr1"},
        "materialize",
    ]

    blobs = mc_command(
        mz,
        " && ".join(
            [
                "mc config host add myminio http://minio-service.default:9000 minio minio123",
                "mc ls myminio/usage",
            ]
        ),
    )
    assert blobs == "potato"
    raise NotImplementedError()
