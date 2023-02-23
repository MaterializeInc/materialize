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
from pathlib import Path
from typing import Any

import requests

from materialize import ui


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


def upload_junit_report(suite: str, junit_report: Path) -> None:
    """Upload a JUnit report to Buildkite Test Analytics.

    Outside of CI, this function does nothing. Inside of CI, the API key for
    Buildkite Test Analytics is expected to be in the environment variable
    `BUILDKITE_TEST_ANALYTICS_API_KEY_{SUITE}`, where `{SUITE}` is the
    upper-snake-cased rendition of the `suite` parameter.

    Args:
        suite: The identifier for the test suite in Buildkite Test Analytics.
        junit_report: The path to the JUnit XML-formatted report file.
    """
    if "CI" not in os.environ:
        return
    ui.header(f"Uploading report for suite {suite!r} to Buildkite Test Analytics")
    suite = suite.upper().replace("-", "_")
    token = os.environ[f"BUILDKITE_TEST_ANALYTICS_API_KEY_{suite}"]
    try:
        res = requests.post(
            "https://analytics-api.buildkite.com/v1/uploads",
            headers={"Authorization": f"Token {token}"},
            json={
                "format": "junit",
                "run_env": {
                    "key": os.environ["BUILDKITE_BUILD_ID"],
                    "CI": "buildkite",
                    "number": os.environ["BUILDKITE_BUILD_NUMBER"],
                    "job_id": os.environ["BUILDKITE_JOB_ID"],
                    "branch": os.environ["BUILDKITE_BRANCH"],
                    "commit_sha": os.environ["BUILDKITE_COMMIT"],
                    "message": os.environ["BUILDKITE_MESSAGE"],
                    "url": os.environ["BUILDKITE_BUILD_URL"],
                },
                "data": junit_report.read_text(),
            },
        )
    except Exception as e:
        print(f"Got exception when uploading analytics: {e}")
    print(res.status_code, res.text)


def get_artifacts() -> Any:
    """Get artifact informations from Buildkite. Outside of CI, this function does nothing."""

    if "CI" not in os.environ:
        return []

    ui.header(f"Getting artifact informations from Buildkite")
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

    res = requests.get(
        f"https://agent.buildkite.com/v3/builds/{build_id}/artifacts/search",
        params=payload,
        headers={"Authorization": f"Token {token}"},
    )

    if res.status_code != 200:
        print(f"Failed to get artifacts: {res.status_code} {res.text}")
        return []

    return res.json()
