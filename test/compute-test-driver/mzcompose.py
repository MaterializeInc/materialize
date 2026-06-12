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

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.minio import Minio, minio_blob_uri

CONSENSUS_URI = "postgres://root@cockroach:26257?options=--search_path=consensus"


class HeadlessDriver(Service):
    def __init__(self, name: str = "headless-driver") -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": "compute-test-driver",
                "environment": [
                    "CLUSTERD_COMPUTE_ADDR=clusterd:2101",
                    f"PERSIST_BLOB_URL={minio_blob_uri()}",
                    f"PERSIST_CONSENSUS_URL={CONSENSUS_URI}",
                    "DRIVER_PUBSUB_BIND=0.0.0.0:6879",
                    # Small shard for the smoke run; raise to stress hydration.
                    "TARGET_BYTES=1000000",
                ],
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


def workflow_default(c: Composition) -> None:
    c.up("cockroach", "minio")
    # clusterd hosts the compute replica; the driver connects to it.
    c.up("clusterd")
    # Run the driver to completion; it exits non-zero on assertion failure.
    c.run("headless-driver")
