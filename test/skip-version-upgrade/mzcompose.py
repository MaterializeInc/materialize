# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test that skipping versions when upgrading will fail.
"""

from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.version_list import (
    get_minor_mz_versions_listed_in_docs,
)

mz_options: dict[MzVersion, str] = {}

SERVICES = [
    Cockroach(setup_materialize=True, in_memory=True),
    Mz(app_password=""),
    Materialized(external_metadata_store=True, metadata_store="cockroach"),
    Testdrive(no_reset=True, metadata_store="cockroach"),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_test_version_skips(c: Composition) -> None:
    current_version = MzVersion.parse_cargo()

    # If the current version is `v0.X.0-dev`, two_minor_releases_before will be `v0.X-2.Y`.
    # where Y is the most recent patch version of the minor version.
    last_two_minor_releases = get_minor_mz_versions_listed_in_docs(
        respect_released_tag=True
    )[-2:]
    two_minor_releases_before = last_two_minor_releases[-2]
    one_minor_release_before = last_two_minor_releases[-1]

    if current_version.major == one_minor_release_before.major:
        assert (
            one_minor_release_before.minor < current_version.minor
        ), "current minor version matches last released minor version from docs; was the release bump merged?"

    print(
        f"Testing that a migration from two minor releases before (={two_minor_releases_before})"
        f" to the current version (={current_version}) should fail"
    )

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            image=f"materialize/materialized:{two_minor_releases_before}",
            external_metadata_store=True,
            options=[
                opt
                for start_version, opt in mz_options.items()
                if two_minor_releases_before >= start_version
            ],
            metadata_store="cockroach",
        )
    ):
        c.up("materialized")
        c.kill("materialized")

    try:
        # This will bring up version `0.X.0-dev`.
        # Note: We actually want to retry this 0 times, but we need to retry at least once so a
        # UIError is raised instead of an AssertionError
        c.up("materialized", max_tries=1)
        raise RuntimeError("skipping versions should fail")
    except UIError:
        # Noting useful in the error message to assert. Ideally we'd check that the error is due to
        # skipping versions.
        pass
