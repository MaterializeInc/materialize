# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Historical role byte-drift repair cases.

Both paths start with a pre-native v26.17.1 catalog and end on the local build.
That catalog conversion is unsupported. Keep the cases visible as skipped,
without running incomplete historical prefixes or substituting a different target.
"""

from unittest import SkipTest

from materialize import buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser

SERVICES = []

UNSUPPORTED_UPGRADE = (
    "Unsupported: pre-native v26.17.1 role-repair upgrade to local native "
    "controllerd/adapterd. No catalog conversion or reset workaround."
)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)
            c.down()

    workflows = buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    )
    c.test_parts(workflows, process)


def workflow_dangling(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Report the unsupported dangling-role repair upgrade."""
    raise SkipTest(UNSUPPORTED_UPGRADE)


def workflow_stale_rows(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Report the unsupported stale-role repair upgrade."""
    raise SkipTest(UNSUPPORTED_UPGRADE)
