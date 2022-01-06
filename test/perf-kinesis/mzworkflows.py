# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Service, Workflow
from materialize.mzcompose.services import Materialized, PrometheusSQLExporter

services = [
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
        },
    ),
]


def workflow_ci(w: Workflow):
    """Run the load generator for one minute as a smoke test."""
    args = ["--total-records=6000", "--records-per-second=100", "--shard-count=2"]
    run(w, args, daemon=False)


def workflow_load_test(w: Workflow):
    """Run the load generator with a hefty load in the background."""
    w.start_services(services=["prometheus-sql-exporter"])
    run(w, args=[], daemon=True)


def run(w: Workflow, args: List[str], daemon: bool):
    w.start_services(services=["materialized"])
    w.wait_for_mz()
    w.run_service(service="perf-kinesis", command=args, daemon=daemon)
