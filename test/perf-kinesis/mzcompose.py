# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Composition, Service
from materialize.mzcompose.services import Materialized, PrometheusSQLExporter

SERVICES = [
    Materialized(),
    PrometheusSQLExporter(),
    Service(
        name="perf-kinesis",
        config={
            "mzbuild": "perf-kinesis",
            "environment": [
                "RUST_LOG=perf-kinesis=debug,info",
                "AWS_ACCESS_KEY_ID",
                "AWS_DEFAULT_REGION=us-east-2",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ],
            "depends_on": ["materialized"],
        },
    ),
]


def workflow_ci(c: Composition) -> None:
    """Run the load generator for one minute as a smoke test."""
    c.run(
        "perf-kinesis",
        "--total-records=6000",
        "--records-per-second=100",
        "--shard-count=2",
    )


def workflow_load_test(c: Composition) -> None:
    """Run the load generator with a hefty load in the background."""
    c.up("prometheus-sql-exporter", "perf-kinesis")
