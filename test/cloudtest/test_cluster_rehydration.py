# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.application import MaterializeApplication


# Test that a crashed (and restarted) cluster replica handles rehydration
# correctly. Currently only tests #18102.
def test_crash_clusterd(mz: MaterializeApplication) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > CREATE CLUSTER c REPLICAS ( r1 (SIZE = '1', INTROSPECTION INTERVAL = 0));

            > CREATE SOURCE counter IN CLUSTER c
                FROM LOAD GENERATOR COUNTER
                (TICK INTERVAL '500ms');

            > DROP SOURCE counter;
            """
        ),
        no_reset=True,
    )

    # Simulate an unexpected clusterd crash.
    pods = mz.kubectl("get", "pods", "-o", "custom-columns=:metadata.name")
    podcount = 0
    for pod in pods.splitlines():
        if "cluster" in pod:
            try:
                mz.kubectl("delete", "pod", pod)
                podcount += 1
            except:
                # It's OK if the pod delete fails --
                # it probably means we raced with a previous test that
                # dropped resources.
                pass
    assert podcount > 0

    mz.testdrive.run(
        input=dedent(
            """
            > SELECT COUNT(*) FROM (SHOW SOURCES);
            0

            > SELECT COUNT(*) FROM mz_internal.mz_source_status_history JOIN ( SELECT source_id AS src, max(occurred_at) AS ocr FROM mz_internal.mz_source_status_history GROUP BY source_id ) AS newest ON newest.src = source_id AND newest.ocr = occurred_at WHERE status = 'running';
            1
            """
        ),
        no_reset=True,
    )
