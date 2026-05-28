# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Smoke test for the in-envd persist committer service.

Spins up Materialize with `persist_consensus_use_committer` enabled so that
envd's own persist traffic flows through the committer, exercises basic SQL
to confirm CaS works, then restarts envd to confirm clusterds reconnect to
the new committer and persisted state survives.
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={
            "persist_consensus_use_committer": "true",
        },
    ),
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition) -> None:
    """Run the smoke and restart workflows."""
    c.workflow("smoke")
    c.workflow("restart")


def workflow_smoke(c: Composition) -> None:
    """Exercise basic SQL with the committer in the read+write path."""
    c.up("materialized")
    c.run_testdrive_files("smoke.td")


def workflow_restart(c: Composition) -> None:
    """Confirm shard state survives an envd restart with the committer on."""
    c.up("materialized")
    c.run_testdrive_files("smoke.td")
    c.kill("materialized")
    c.up("materialized")
    c.run_testdrive_files("after_restart.td")
