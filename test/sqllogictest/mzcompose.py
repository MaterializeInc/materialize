# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import ROOT, ci_util
from materialize.mzcompose import Composition
from materialize.mzcompose.services import Cockroach, SqlLogicTest

SERVICES = [Cockroach(in_memory=True), SqlLogicTest()]


def workflow_default(c: Composition) -> None:
    "Run fast SQL logic tests"
    run_sqllogictest(c, "ci/test/slt-fast.sh")


def workflow_sqllogictest(c: Composition) -> None:
    "Run slow SQL logic tests"
    run_sqllogictest(c, "ci/slt/slt.sh")


def run_sqllogictest(c: Composition, command: str) -> None:
    c.up("cockroach")

    shard = os.environ.get("BUILDKITE_PARALLEL_JOB")
    shard_count = os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT")

    try:
        junit_report = ci_util.junit_report_filename(c.name)

        args = [
            "sqllogictest",
            command,
            f"--junit-report={junit_report}",
            "--postgres-url=postgres://root@cockroach:26257",
        ]

        if shard:
            args += [f"--shard={shard}"]
        if shard_count:
            args += [f"--shard-count={shard_count}"]

        c.run(*args)
    finally:
        ci_util.upload_junit_report(c.name, ROOT / junit_report)
