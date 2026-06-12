# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Headless compute driver: build an index over a persist shard via a real
clusterd, with no environmentd. The driver hosts persist PubSub; clusterd is
pointed at it via `mz_service`."""

from materialize import ui
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.minio import Minio, minio_blob_uri

CONSENSUS_URI = "postgres://root@cockroach:26257?options=--search_path=consensus"


# Where the mounted scripts land inside the driver container.
SCRIPTS_DIR = "/workdir/scripts"


class HeadlessDriver(Service):
    def __init__(self, name: str = "headless-driver") -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": "clusterd-test-driver",
                "environment": [
                    "CLUSTERD_COMPUTE_ADDR=clusterd:2101",
                    f"PERSIST_BLOB_URL={minio_blob_uri()}",
                    f"PERSIST_CONSENSUS_URL={CONSENSUS_URI}",
                    "DRIVER_PUBSUB_BIND=0.0.0.0:6879",
                ],
                # Mount the composition dir so scripts are readable at
                # /workdir/scripts (the convention testdrive uses for its files);
                # the driver reads the file named by DRIVER_SCRIPT.
                "volumes": [".:/workdir"],
                "ports": [6879],
            },
        )


SERVICES = [
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    # Point clusterd's persist PubSub at the driver, which hosts it.
    Clusterd(mz_service="headless-driver"),
    HeadlessDriver(),
]


# The scenarios, each a JSON command script under scripts/. `multi_dataflow`
# reproduces a current limitation and exits 0 by design (tolerant awaits); the
# others assert via expect_count/expect_error and fail the run on mismatch.
SCRIPTS = [
    "index.jsonl",
    "deep_history.jsonl",
    "side_effects.jsonl",
    "multi_dataflow.jsonl",
    "reconciliation.jsonl",
    "error_behavior.jsonl",
]


def workflow_default(c: Composition) -> None:
    c.up("cockroach", "minio")
    for i, script in enumerate(SCRIPTS):
        # Buildkite collapsible section per scenario.
        ui.section(f"Running scenario {script}")
        # Restart clusterd between scenarios for a clean compute state; the
        # scripts reuse GlobalIds and would otherwise collide.
        if i > 0:
            c.kill("clusterd")
        c.up("clusterd")
        # Run the driver to completion; it exits non-zero on assertion failure.
        # use_aliases gives the run container the `headless-driver` network
        # alias so clusterd can reach the PubSub server it hosts.
        c.run(
            "headless-driver",
            env_extra={"DRIVER_SCRIPT": f"{SCRIPTS_DIR}/{script}"},
            use_aliases=True,
        )
