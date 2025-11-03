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
    get_all_mz_versions,
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
    
    Note: This function does NOT verify Docker images exist, for speed.
    The assumption is that released versions have published images.
    """
    # Get all versions from git tags (fast, no Docker check)
    all_versions = get_all_mz_versions(newest_first=True)
    
    # Debug: Print last 5 versions from git tags
    print(f"DEBUG: Last 5 released versions: {[str(v) for v in all_versions[:5]]}")
    
    # Map versions to deployment stages based on array position
    # cloud-forward: newest version (index 0) (version in staging)
    # cloud-current: the version released before the latest one (version in prod)
    # cloud-backward: two versions back before the latest released version (version in staging)
    
    cloud_forward = all_versions[0] if len(all_versions) > 0 else None
    cloud_current = all_versions[1] if len(all_versions) > 2 else None
    cloud_backward = all_versions[2] if len(all_versions) > 4 else None
    
    # Self-managed LTS: Get from Helm chart (fast, already cached)
    sm_versions = get_self_managed_versions()
    # Currently targeting v25.2.x series as LTS
    sm_lts = max(
        (v for v in sm_versions if v.major == 25 and v.minor == 2),
        default=max(sm_versions) if sm_versions else cloud_backward,
    )
    
    return {
        "cloud-backward": cloud_backward,
        "cloud-current": cloud_current,
        "cloud-forward": cloud_forward,
        "sm-lts": sm_lts,
    }

