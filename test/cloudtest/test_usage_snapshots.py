# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time

import pytest

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s.minio import mc_command
from materialize.cloudtest.wait import wait


def test_usage_snapshots(mz: MaterializeApplication) -> None:
    start = time.time()
    print("Beginning test session...")
    mz.environmentd.sql(f"CREATE CLUSTER c REPLICAS (cr1 (SIZE '2'))")
    print(" - created cluster, now sleeping...")
    time.sleep(3)
    print(" - woke up after 3 seconds, now querying audit log")
    audit_rows = mz.environmentd.sql_query(
        "SELECT id, event_type, object_type, event_details, user FROM mz_audit_events"
    )
    assert audit_rows[0][1:] == ["create", "cluster", {"name": "c"}, "materialize"]
    assert audit_rows[1][1:] == [
        "create",
        "cluster-replica",
        {"cluster_name": "c", "logical_size": "2", "replica_name": "cr1"},
        "materialize",
    ]
    print(" - audit logs look good! Now listing blobs in minio...")
    blobs = mc_command(
        mz,
        "mc config host add myminio http://minio-service.default:9000 minio minio123",
        "mc --json ls -r myminio/usage/usage/",
        output=True
    )
    # The first line is minio-client output of the form "Added successfully", the rest are NDJSON
    blobs = [json.loads(l) for l in blobs.splitlines()[1:]]
    # there should be ~as many entries as there have been seconds since launch
    seconds_since_launch = (time.time() - start)
    assert len(blobs) == pytest.approx(seconds_since_launch, rel=3)
    assert blobs == "potato"
