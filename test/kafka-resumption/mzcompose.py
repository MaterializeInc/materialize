# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import Optional

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Storaged,
    Testdrive,
    Toxiproxy,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Storaged(),
    Toxiproxy(),
    Testdrive(default_timeout="120s"),
]


def workflow_default(c: Composition) -> None:
    c.workflow("sink-networking")
    c.workflow("source-resumption")


#
# Test the kafka sink resumption logic in the presence of networking problems
#
def workflow_sink_networking(c: Composition) -> None:
    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy"]
    )
    c.wait_for_materialized()

    seed = random.getrandbits(16)
    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-limit-connection.td",
            "toxiproxy-timeout.td",
            "toxiproxy-timeout-hold.td",
        ]
    ):
        c.start_and_wait_for_tcp(["toxiproxy"])
        c.run(
            "testdrive",
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
    """Test creating sources in a remote storaged process."""

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
    ):
        c.start_and_wait_for_tcp(
            services=["materialized", "zookeeper", "kafka", "storaged"]
        )

        c.run("testdrive", "source-resumption/setup.td")
        c.run("testdrive", "source-resumption/verify.td")
        assert (
            find_source_resume_upper(
                c,
                "0",
            )
            == None
        )

        c.kill("storaged")
        c.up("storaged")
        c.sleep(10)

        # Verify the same data is query-able, and that we can make forward progress
        c.run("testdrive", "source-resumption/verify.td")
        c.run("testdrive", "source-resumption/re-ingest-and-verify.td")

        # the first storaged instance ingested 3 messages, so our
        # upper is at the 4th offset (0-indexed)
        assert (
            find_source_resume_upper(
                c,
                "0",
            )
            == 3
        )


def find_source_resume_upper(c: Composition, partition_id: str) -> Optional[int]:
    metrics = c.exec("storaged", "curl", "localhost:6878/metrics", capture=True).stdout

    if metrics is None:
        return None

    for metric in metrics.splitlines():
        if metric.startswith("mz_source_resume_upper"):
            labels, value = metric[len("mz_source_resume_upper") :].split(" ", 2)

            # prometheus doesn't use real json, so we do some hacky nonsense here :(
            if labels[len("{partition_id=") :].startswith(f'"{partition_id}"'):
                return int(value)

    return None
