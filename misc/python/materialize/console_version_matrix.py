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


def get_console_test_versions() -> dict[str, MzVersion | None]:
    """
    Get all versions that should be tested for console.

    Uses existing version_list.py infrastructure to determine:
    - cloud-backward: Last released minor version (from git tags)
    - cloud-current: Current source (None)
    - cloud-forward: Current source (None)
    - sm-lts: Latest self-managed LTS minor version

    Returns:
        Dictionary mapping test name to version (None = current source)

    Note: Uses get_published_minor_mz_versions which verifies Docker images exist.
    """
    # Get the latest patch version for each minor version (with Docker image verification)
    # Limit to 5 versions for speed
    minor_versions = get_published_minor_mz_versions(newest_first=True, limit=5)

    # Debug: Print minor versions
    print(f"DEBUG: Minor versions (newest first): {[str(v) for v in minor_versions]}")
    
    # Map versions to deployment stages based on array position
    # cloud-forward: newest version (index 0) (version in staging)
    # cloud-current: the version released before the latest one (version in prod)
    # cloud-backward: two versions back before the latest released version (version in staging)
    
    cloud_forward = minor_versions[0] if len(minor_versions) > 0 else None
    cloud_current = minor_versions[1] if len(minor_versions) > 1 else None
    cloud_backward = minor_versions[2] if len(minor_versions) > 2 else None
    
    # Self-managed LTS: Get the latest self-managed version from Helm chart
    sm_versions = get_self_managed_versions()
    sm_lts = max(sm_versions) if sm_versions else cloud_backward
    
    return {
        "cloud-backward": cloud_backward,
        "cloud-current": cloud_current,
        "cloud-forward": cloud_forward,
        "sm-lts": sm_lts,
    }

