# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Version matrix for console testing - uses existing version_list infrastructure.
"""

from materialize.mz_version import MzVersion
from materialize.version_list import (
    get_published_minor_mz_versions,
    get_self_managed_versions,
)


def get_console_test_versions() -> dict[str, MzVersion]:
    """
    Get all versions that should be tested for console.

    Uses existing version_list.py infrastructure to determine:
    - latest: Most recent minor version
    - previous: One minor version back
    - older: Two minor versions back
    - self-managed: Latest self-managed version from Helm chart

    Returns:
        Dictionary mapping test name to version

    Note: Uses get_published_minor_mz_versions which verifies Docker images exist.
    """
    # Get the latest patch version for each minor version (with Docker image verification)
    # Limit to 5 versions for speed
    minor_versions = get_published_minor_mz_versions(newest_first=True, limit=5)

    assert (
        len(minor_versions) >= 3
    ), f"Expected at least 3 minor versions, got {len(minor_versions)}"

    latest = minor_versions[0]
    previous = minor_versions[1]
    older = minor_versions[2]

    # Self-managed: Get the latest self-managed version from Helm chart
    sm_versions = get_self_managed_versions()
    assert sm_versions, "Expected at least one self-managed version"
    self_managed = max(sm_versions)

    return {
        "latest": latest,
        "previous": previous,
        "older": older,
        "self-managed": self_managed,
    }
