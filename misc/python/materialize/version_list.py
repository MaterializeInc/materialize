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

import frontmatter

from materialize import git
from materialize.git import get_version_tags
from materialize.mz_version import MzVersion

MZ_ROOT = Path(os.environ["MZ_ROOT"])

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


def get_all_mz_versions() -> list[MzVersion]:
    return [
        version
        for version in get_version_tags(version_type=MzVersion)
        if version not in INVALID_VERSIONS
    ]


def get_all_minor_mz_versions() -> list[MzVersion]:
    """Return the latest patch version for every minor version."""

    # sorted in descending order
    all_versions = get_all_mz_versions()
    minor_versions: dict[str, MzVersion] = {}

    for version in all_versions:
        minor_version = f"{version.major}.{version.minor}"
        if minor_version not in minor_versions.keys():
            minor_versions[minor_version] = version

    assert len(minor_versions) > 0
    return sorted(minor_versions.values())


def get_previous_mz_version(
    version: MzVersion, excluded_versions: set[MzVersion] | None = None
) -> MzVersion:
    if excluded_versions is None:
        excluded_versions = set()

    if version.prerelease is not None and len(version.prerelease) > 0:
        # simply drop the prerelease, do not try to find a decremented version
        found_version = MzVersion.create(version.major, version.minor, version.patch)

        if found_version not in excluded_versions:
            return found_version
        else:
            # start searching with this version
            version = found_version

    all_versions: list[MzVersion] = get_version_tags(version_type=type(version))
    all_suitable_previous_versions = [
        v
        for v in all_versions
        if v < version
        and (v.prerelease is None or len(v.prerelease) == 0)
        and v not in INVALID_VERSIONS
        and v not in excluded_versions
    ]
    return max(all_suitable_previous_versions)


class VersionList:
    def __init__(self) -> None:
        self.versions: list[MzVersion]
        assert False

    def all_versions(self) -> list[MzVersion]:
        return self.versions

    def minor_versions(self) -> list[MzVersion]:
        """Return the latest patch version for every minor version."""
        minor_versions = {}
        for version in self.versions:
            minor_versions[f"{version.major}.{version.minor}"] = version

        assert len(minor_versions) > 0
        return sorted(minor_versions.values())

    def patch_versions(self, minor_version: MzVersion) -> list[MzVersion]:
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

    >>> len(VersionsFromGit().patch_versions(minor_version=MzVersion.parse_mz("v0.52.0")))
    4

    >>> min(VersionsFromGit().all_versions())
    MzVersion(major=0, minor=1, patch=0, prerelease='rc', build=None)
    """

    def __init__(self) -> None:
        self.versions = list(
            set(git.get_version_tags(version_type=MzVersion, fetch=True))
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

    >>> len(VersionsFromDocs().patch_versions(minor_version=MzVersion.parse_mz("v0.52.0")))
    4

    >>> min(VersionsFromDocs().all_versions())
    MzVersion(major=0, minor=27, patch=0, prerelease=None, build=None)
    """

    def __init__(self) -> None:
        files = Path(MZ_ROOT / "doc" / "user" / "content" / "releases").glob("v*.md")
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
