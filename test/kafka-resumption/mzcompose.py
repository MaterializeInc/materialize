# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Redpanda(),
    Materialized(),
    Clusterd(),
    Toxiproxy(),
    Testdrive(default_timeout="120s"),
]


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


#
# Test the kafka sink resumption logic in the presence of networking problems
#
def workflow_sink_networking(c: Composition) -> None:
    c.up("redpanda", "materialized", "toxiproxy")

    seed = random.getrandbits(16)
    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-limit-connection.td",
            "toxiproxy-timeout.td",
            "toxiproxy-timeout-hold.td",
        ]
    ):
        c.up("toxiproxy")
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}{i}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}{i}",
            "sink-networking/setup.td",
            f"sink-networking/{failure_mode}",
            "sink-networking/during.td",
            "sink-networking/sleep.td",
            "sink-networking/toxiproxy-restore-connection.td",
            "sink-networking/verify-success.td",
            "sink-networking/cleanup.td",
        )
        c.kill("toxiproxy")


def workflow_source_resumption(c: Composition) -> None:
    """Test creating sources in a remote clusterd process."""

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
    ):
        c.up("materialized", "redpanda", "clusterd")

        c.run_testdrive_files("source-resumption/setup.td")
        c.run_testdrive_files("source-resumption/verify.td")

        # Disabled due to https://github.com/MaterializeInc/materialize/issues/20819
        # assert (
        #    find_source_resume_upper(
        #        c,
        #        "0",
        #    )
        #    == None
        # )

        c.kill("clusterd")
        c.up("clusterd")
        c.sleep(10)

        # Verify the same data is query-able, and that we can make forward progress
        c.run_testdrive_files("source-resumption/verify.td")
        c.run_testdrive_files("source-resumption/re-ingest-and-verify.td")

        # the first clusterd instance ingested 3 messages, so our
        # upper is at the 4th offset (0-indexed)

        # Disabled due to https://github.com/MaterializeInc/materialize/issues/20819
        # assert (
        #    find_source_resume_upper(
        #        c,
        #        "0",
        #    )
        #    == 3
        # )


def find_source_resume_upper(c: Composition, partition_id: str) -> int | None:
    metrics = c.exec("clusterd", "curl", "localhost:6878/metrics", capture=True).stdout

    if metrics is None:
        return None

    for metric in metrics.splitlines():
        if metric.startswith("mz_source_resume_upper"):
            labels, value = metric[len("mz_source_resume_upper") :].split(" ", 2)

            # prometheus doesn't use real json, so we do some hacky nonsense here :(
            if labels[len("{partition_id=") :].startswith(f'"{partition_id}"'):
                return int(value)

    return None


def workflow_sink_queue_full(c: Composition) -> None:
    """Similar to the sink-networking workflow, but with 11 million rows (more then the 11 million defined as queue.buffering.max.messages) and only creating the sink after these rows are ingested into Mz. Triggers #24936"""
    seed = random.getrandbits(16)
    c.up("redpanda", "materialized", "toxiproxy")
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
        "sink-queue-full/setup.td",
        "sink-queue-full/during.td",
        "sink-queue-full/verify-success.td",
        "sink-queue-full/cleanup.td",
    )
