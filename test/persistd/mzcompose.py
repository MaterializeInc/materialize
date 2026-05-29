# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Smoke test that routes envd's persist consensus traffic through a separate
persistd container. Confirms the standalone committer accepts gRPC traffic
end-to-end and that persisted state survives an envd restart while persistd
keeps running.
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    metadata_store_services,
)
from materialize.mzcompose.services.persistd import Persistd
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    *metadata_store_services(),
    Persistd(
        depends_on=[METADATA_STORE],
        consensus_url=(
            f"postgres://root@{METADATA_STORE}:26257?options=--search_path=consensus"
        ),
    ),
    Materialized(
        additional_system_parameter_defaults={
            "persist_consensus_use_committer": "true",
        },
        # Materialized would otherwise auto-attach its own paired Persistd
        # companion. Disable it here so this composition exercises the
        # explicitly-configured standalone persistd container declared in
        # SERVICES above.
        external_persist_committer=False,
        depends_on=["persistd"],
        environment_extra=[
            "MZ_PERSIST_COMMITTER_URL=http://persistd:6882",
            "MZ_EXTERNAL_PERSIST_COMMITTER=true",
        ],
    ),
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition) -> None:
    """Run every non-default workflow."""
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_smoke(c: Composition) -> None:
    """Exercise basic SQL with persistd in the read+write path."""
    c.up("persistd", "materialized")
    c.run_testdrive_files("smoke.td")


def workflow_restart(c: Composition) -> None:
    """Confirm shard state survives an envd restart while persistd stays up."""
    c.up("persistd", "materialized")
    c.run_testdrive_files("smoke.td")
    c.kill("materialized")
    c.up("materialized")
    c.run_testdrive_files("after_restart.td")
