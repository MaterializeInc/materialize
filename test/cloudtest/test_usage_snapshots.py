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
import textwrap

import pytest

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s.minio import mc_command
from materialize.cloudtest.wait import wait


def test_usage_snapshots(mz: MaterializeApplication) -> None:
    start = time.time()
    print("Beginning test session...")
    mz.environmentd.sql(f"CREATE CLUSTER c REPLICAS (cr1 (SIZE '2'))")
    print(" - created cluster, now sleeping...")
    time.sleep(2)
    print(" - woke up after 2 seconds, now creating sources...")
    mz.testdrive.run_string(
        textwrap.dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE SOURCE source1
              FROM KAFKA BROKER '${testdrive.kafka-addr}'
              TOPIC 'testdrive-test-${testdrive.seed}'
              FORMAT BYTES
              ENVELOPE NONE;
            """
        )
    )
    print(" - created a source, now sleeping...")
    time.sleep(2)
    print(" - woke up after 2 seconds, now querying audit log")
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
    breakpoint()
    assert len(blobs) == pytest.approx(seconds_since_launch, rel=3)
    assert blobs == "potato"

    # TODO: now ensure the entries correspond to the activity!
    # TODO: test catching up to missed chunks after e.g. a restart

query ITTTT
SELECT id, event_type, object_type, event_details, user FROM mz_audit_events ORDER BY id
----
1  create  cluster  {"name":"foo"}  materialize
2  create  cluster-replica  {"cluster_name":"foo","logical_size":"1","replica_name":"r"}  materialize
1  create  cluster  {"id":2,"name":"foo"}  materialize
2  create  cluster-replica  {"cluster_id":2,"cluster_name":"foo","logical_size":"1","replica_name":"r"}  materialize
3  create  materialized-view  {"database":"materialize","item":"v2","schema":"public"}  materialize
4  create  view  {"database":"materialize","item":"unmat","schema":"public"}  materialize
5  create  index  {"database":"materialize","item":"t_primary_idx","schema":"public"}  materialize
6  alter  view  {"new_name":"renamed","previous_name":{"database":"materialize","item":"unmat","schema":"public"}}  materialize
7  drop  materialized-view  {"database":"materialize","item":"v2","schema":"public"}  materialize
8  create  materialized-view  {"database":"materialize","item":"v2","schema":"public"}  materialize
9  create  index  {"database":"materialize","item":"renamed_primary_idx","schema":"public"}  materialize
10  drop  index  {"database":"materialize","item":"renamed_primary_idx","schema":"public"}  materialize
11  drop  view  {"database":"materialize","item":"renamed","schema":"public"}  materialize
12  create  source  {"database":"materialize","item":"s","schema":"public"}  materialize
13  drop  cluster-replica  {"cluster_id":2,"cluster_name":"foo","replica_name":"r"}  materialize
14  drop  cluster  {"id":2,"name":"foo"}  materialize
