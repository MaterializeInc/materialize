# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import ci_util
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Redpanda,
    SchemaRegistry,
    Storaged,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Materialized(),
    Testdrive(),
    Storaged(
        name="storaged",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test `REMOTE` storageds"""
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    dependencies = ["materialized", "storaged"]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]
    c.start_and_wait_for_tcp(
        services=dependencies,
    )

    try:
        junit_report = ci_util.junit_report_filename(c.name)
        c.run("testdrive", f"--junit-report={junit_report}", *args.files)
    finally:
        ci_util.upload_junit_report("testdrive", Path(__file__).parent / junit_report)
