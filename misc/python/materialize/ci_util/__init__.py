# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utility functions only useful in CI."""

import os
import time
from pathlib import Path
from typing import Any

import requests
from semver.version import VersionInfo

from materialize import MZ_ROOT, buildkite, cargo, ui


def junit_report_filename(suite: str) -> Path:
    """Compute the JUnit report filename for the specified test suite.

    See also `upload_test_report`. In CI, the filename will include the
    Buildkite job ID.

    Args:
        suite: The identifier for the test suite in Buildkite Test Analytics.
    """
    filename = f"junit_{suite}"
    if "BUILDKITE_JOB_ID" in os.environ:
        filename += "_" + os.environ["BUILDKITE_JOB_ID"]
    return Path(f"{filename}.xml")


def get_artifacts() -> Any:
    """Get artifact informations from Buildkite. Outside of CI, this function does nothing."""

    if not buildkite.is_in_buildkite():
        return []

    ui.section("Getting artifact informations from Buildkite")
    build = os.environ["BUILDKITE_BUILD_NUMBER"]
    build_id = os.environ["BUILDKITE_BUILD_ID"]
    job = os.environ["BUILDKITE_JOB_ID"]
    token = os.environ["BUILDKITE_AGENT_ACCESS_TOKEN"]

    payload = {
        "query": "*",
        "step": job,
        "build": build,
        "state": "finished",
        "includeRetriedJobs": "false",
        "includeDuplicates": "false",
    }

    attempts = 10
    res = None
    for attempt in range(attempts):
        try:
            res = requests.get(
                f"https://agent.buildkite.com/v3/builds/{build_id}/artifacts/search",
                params=payload,
                headers={"Authorization": f"Token {token}"},
            )
            res.raise_for_status()
            break
        except:
            if attempt == attempts - 1:
                raise
            time.sleep(5)

    assert res
    if res.status_code != 200:
        print(f"Failed to get artifacts: {res.status_code} {res.text}")
        return []

    return res.json()


def get_mz_version(workspace: cargo.Workspace | None = None) -> VersionInfo:
    """Get the current Materialize version from Cargo.toml."""

    if not workspace:
        workspace = cargo.Workspace(MZ_ROOT)
    return VersionInfo.parse(workspace.crates["mz-environmentd"].version_string)
