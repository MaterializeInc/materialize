# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import os
from pathlib import Path
from typing import List

import frontmatter

from materialize.git import get_version_tags
from materialize.util import MzVersion

ROOT = Path(os.environ["MZ_ROOT"])

# not released on Docker
INVALID_VERSIONS = {
    MzVersion.parse_mz("v0.52.1"),
    MzVersion.parse_mz("v0.55.1"),
    MzVersion.parse_mz("v0.55.2"),
    MzVersion.parse_mz("v0.55.3"),
    MzVersion.parse_mz("v0.55.4"),
    MzVersion.parse_mz("v0.55.5"),
    MzVersion.parse_mz("v0.55.6"),
    MzVersion.parse_mz("v0.56.0"),
    MzVersion.parse_mz("v0.57.1"),
    MzVersion.parse_mz("v0.57.2"),
    MzVersion.parse_mz("v0.57.5"),
    MzVersion.parse_mz("v0.57.6"),
}


class VersionList:
    def __init__(self) -> None:
        self.versions: List[MzVersion]
        assert False

    def all_versions(self) -> List[MzVersion]:
        return self.versions

    def minor_versions(self) -> List[MzVersion]:
        """Return the latest patch version for every minor version."""
        minor_versions = {}
        for version in self.versions:
            minor_versions[f"{version.major}.{version.minor}"] = version

        assert len(minor_versions) > 0
        return sorted(minor_versions.values())

    def patch_versions(self, minor_version: MzVersion) -> List[MzVersion]:
        """Return all patch versions within the given minor version."""
        patch_versions = []
        for version in self.versions:
            if (
                version.major == minor_version.major
                and version.minor == minor_version.minor
            ):
                patch_versions.append(version)

        assert len(patch_versions) > 0
        return sorted(patch_versions)


class VersionsFromGit(VersionList):
    """Materialize versions as tagged in Git.

    >>> len(VersionsFromGit().all_versions()) > 0
    True

    >>> len(VersionsFromGit().minor_versions()) > 0
    True

    >>> len(VersionsFromGit().patch_versions(minor_version=MzVersion.parse("0.52.0")))
    4

    >>> min(VersionsFromGit().all_versions())
    MzVersion(major=0, minor=1, patch=0, prerelease='rc', build=None)
    """

    def __init__(self) -> None:
        self.versions = list(
            {MzVersion.from_semver(t) for t in get_version_tags(fetch=True)}
            - INVALID_VERSIONS
        )
        self.versions.sort()


class VersionsFromDocs(VersionList):
    """Materialize versions as listed in doc/user/content/versions

    Only versions that declare `versiond: true` in their
    frontmatter are considered.

    >>> len(VersionsFromDocs().all_versions()) > 0
    True

    >>> len(VersionsFromDocs().minor_versions()) > 0
    True

    >>> len(VersionsFromDocs().patch_versions(minor_version=MzVersion.parse("0.52.0")))
    4

    >>> min(VersionsFromDocs().all_versions())
    MzVersion(major=0, minor=27, patch=0, prerelease=None, build=None)
    """

    def __init__(self) -> None:
        files = Path(ROOT / "doc" / "user" / "content" / "releases").glob("v*.md")
        self.versions = []
        for f in files:
            base = f.stem
            metadata = frontmatter.load(f)
            if not metadata.get("released", False):
                continue

            current_patch = metadata.get("patch", 0)

            for patch in range(current_patch + 1):
                version = MzVersion.parse_mz(f"{base}.{patch}")
                if version not in INVALID_VERSIONS:
                    self.versions.append(version)

        assert len(self.versions) > 0
        self.versions.sort()
