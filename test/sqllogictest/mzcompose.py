# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import MZ_ROOT, ci_util
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.sql_logic_test import SqlLogicTest

SERVICES = [Cockroach(in_memory=True), SqlLogicTest()]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    "Run fast SQL logic tests"
    run_sqllogictest(c, parser, "ci/test/slt-fast.sh")


def workflow_sqllogictest(c: Composition, parser: WorkflowArgumentParser) -> None:
    "Run slow SQL logic tests"
    run_sqllogictest(
        c,
        parser,
        "ci/slt/slt.sh",
    )


def run_sqllogictest(
    c: Composition, parser: WorkflowArgumentParser, command: str
) -> None:
    parser.add_argument("--replicas", default=2, type=int)
    args = parser.parse_args()

    c.up("cockroach")

    shard = os.environ.get("BUILDKITE_PARALLEL_JOB")
    shard_count = os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT")

    junit_report = ci_util.junit_report_filename(c.name)

    cmd_args = [
        "sqllogictest",
        command,
        f"--junit-report={junit_report}",
        "--postgres-url=postgres://root@cockroach:26257",
        f"--replicas={args.replicas}",
    ]

    if shard:
        cmd_args += [f"--shard={shard}"]
    if shard_count:
        cmd_args += [f"--shard-count={shard_count}"]

    try:
        c.run(*cmd_args)
    finally:
        ci_util.upload_junit_report(c.name, MZ_ROOT / junit_report)
