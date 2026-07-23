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
from materialize.mzcompose.composition import Service as ServiceName
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    metadata_store_companions,
)
from materialize.mzcompose.services.minio import Minio, minio_blob_uri

# Persist consensus lives in the metadata store configured through the
# environment (default `postgres-metadata`, or `EXTERNAL_METADATA_STORE`). Both
# the SQL-backed stores listen on 26257 under a hostname equal to their name and
# have `setup_materialize` create the `consensus` schema. FoundationDB is
# excluded: persist *does* have an `FdbConsensus`, but it is behind the
# `foundationdb` cargo feature that the clusterd-test-driver/clusterd images do
# not build, and its consensus URI is a `foundationdb://` cluster file rather
# than the shared `postgres://…:26257` form.
if METADATA_STORE not in ("cockroach", "postgres-metadata"):
    raise ValueError(
        f"clusterd-test-driver needs a SQL metadata store for persist consensus, "
        f"got EXTERNAL_METADATA_STORE={METADATA_STORE}"
    )
CONSENSUS_URI = (
    f"postgres://root@{METADATA_STORE}:26257?options=--search_path=consensus"
)


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
    # The metadata store backing persist consensus (CockroachDB or Postgres),
    # selected through the environment rather than hardcoded. We force
    # `external_metadata_store=True`: the default gate is meant for an
    # `environmentd` that would otherwise use its internal store, but this
    # composition has no `environmentd` and always needs a standalone store for
    # persist consensus, so the companion must materialize regardless.
    *metadata_store_companions(external_metadata_store=True),
    Minio(setup_materialize=True),
    # Point clusterd's persist PubSub at the driver, which hosts it.
    Clusterd(mz_service="headless-driver"),
    HeadlessDriver(),
]


# The scenarios, each a text command script under scripts/ whose `----` blocks
# are golden assertions. `multi_dataflow` reproduces a current limitation but
# stays deterministic via `allow-timeout`; the others fail the run on a mismatch.
SCRIPTS = [
    "index.spec",
    "deep_history.spec",
    "side_effects.spec",
    "multi_dataflow.spec",
    "reconciliation.spec",
    "error_behavior.spec",
    "reduce.spec",
    "materialized_view.spec",
    "subscribe.spec",
    "join.spec",
    "index_and_mv.spec",
    "create_time_config.spec",
]

# Scenarios run against a clusterd with the interactive compute runtime enabled
# (see `workflow_two_runtime_compute`). Kept separate from `SCRIPTS` so the
# single-runtime scenarios above stay byte-for-byte unchanged.
TWO_RUNTIME_SCRIPTS = [
    "two_runtime_index.spec",
    "two_runtime_query_dataflow.spec",
]


def workflow_default(c: Composition) -> None:
    c.up(METADATA_STORE, "minio", ServiceName("headless-driver", idle=True))
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
    # Also exercise every other workflow (the two-runtime compute path) so the
    # CI job, which runs this composition's default workflow, covers them too.
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_two_runtime_compute(c: Composition) -> None:
    """Run the two-runtime scenarios against a clusterd started with a second,
    interactive compute runtime configured. `Clusterd(interactive_compute=True)`
    sets `CLUSTERD_INTERACTIVE_COMPUTE_TIMELY_CONFIG`, which turns on the
    multiplexer that fronts both runtimes on the same `:2101` endpoint this
    driver already connects to, so no driver changes are needed to reach the
    interactive runtime.
    """
    c.up(METADATA_STORE, "minio", ServiceName("headless-driver", idle=True))
    with c.override(Clusterd(mz_service="headless-driver", interactive_compute=True)):
        for i, script in enumerate(TWO_RUNTIME_SCRIPTS):
            ui.section(f"Running two-runtime scenario {script}")
            if i > 0:
                c.kill("clusterd")
            c.up("clusterd")
            c.run(
                "headless-driver",
                env_extra={"DRIVER_SCRIPT": f"{SCRIPTS_DIR}/{script}"},
                use_aliases=True,
            )
