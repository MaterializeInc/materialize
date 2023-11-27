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

from materialize import buildkite, git
from materialize.docker import (
    commit_to_image_tag,
    image_of_commit_exists,
    image_of_release_version_exists,
    version_to_image_tag,
)
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


"""
Git revisions that are based on commits listed as keys require at least the version specified in the value.
Note that specified versions do not necessarily need to be already published.
Commits must be ordered descending by their date.
"""
MIN_ANCESTOR_MZ_VERSION_PER_COMMIT: dict[str, MzVersion] = {
    # insert newer commits at the top
    # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
    "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0")
}


def resolve_ancestor_image_tag() -> str:
    image_tag, context = _resolve_ancestor_image_tag()
    print(f"Using {image_tag} as image tag for ancestor (context: {context})")
    return image_tag


def _resolve_ancestor_image_tag() -> tuple[str, str]:
    if buildkite.is_in_buildkite():
        return _resolve_ancestor_image_tag_when_in_buildkite()
    else:
        return _resolve_ancestor_image_tag_when_running_locally()


def _resolve_ancestor_image_tag_when_in_buildkite() -> tuple[str, str]:
    if buildkite.is_in_pull_request():
        # return the merge base
        common_ancestor_commit = buildkite.get_merge_base()
        if image_of_commit_exists(common_ancestor_commit):
            return (
                commit_to_image_tag(common_ancestor_commit),
                "merge base of pull request",
            )
        else:
            return (
                version_to_image_tag(get_latest_published_version()),
                "latest release because image of merge base of pull request not available",
            )
    elif git.is_on_release_version():
        # return the previous release
        tagged_release_version = git.get_tagged_release_version(version_type=MzVersion)
        assert tagged_release_version is not None
        previous_release_version = get_previous_published_version(
            tagged_release_version
        )
        return (
            version_to_image_tag(previous_release_version),
            f"previous release because on release branch {tagged_release_version}",
        )
    else:
        latest_published_version = get_latest_published_version()
        override_commit = _get_override_commit_instead_of_version(
            latest_published_version
        )

        if override_commit is not None:
            # use the commit instead of the latest release
            return (
                commit_to_image_tag(override_commit),
                f"commit override instead of latest release ({latest_published_version})",
            )

        # return the latest release
        return (
            version_to_image_tag(latest_published_version),
            "latest release because not in a pull request and not on a release branch",
        )


def _resolve_ancestor_image_tag_when_running_locally() -> tuple[str, str]:
    if git.is_on_release_version():
        # return the previous release
        tagged_release_version = git.get_tagged_release_version(version_type=MzVersion)
        assert tagged_release_version is not None
        previous_release_version = get_previous_published_version(
            tagged_release_version
        )
        return (
            version_to_image_tag(previous_release_version),
            f"previous release because on local release branch {tagged_release_version}",
        )
    elif git.is_on_main_branch():
        # return the latest release
        latest_published_version = get_latest_published_version()
        override_commit = _get_override_commit_instead_of_version(
            latest_published_version
        )

        if override_commit is not None:
            # use the commit instead of the latest release
            return (
                commit_to_image_tag(override_commit),
                f"commit override instead of latest release ({latest_published_version})",
            )

        return (
            version_to_image_tag(latest_published_version),
            "latest release because on local main branch",
        )
    else:
        # return the merge base
        common_ancestor_commit = buildkite.get_merge_base()
        if image_of_commit_exists(common_ancestor_commit):
            return (
                commit_to_image_tag(common_ancestor_commit),
                "merge base of local non-main branch",
            )
        else:
            return (
                version_to_image_tag(get_latest_published_version()),
                "latest release because image of merge base of local non-main branch not available",
            )


def _get_override_commit_instead_of_version(
    latest_published_version: MzVersion,
) -> str | None:
    """
    If a commit specifies a mz version as prerequisite (to avoid regressions) that is newer than the
    provided latest version (i.e., prerequisite not satisfied by the latest version), then return
    that commit's hash if the commit contained in the current state.
    Otherwise, return none.
    """
    for (
        commit_hash,
        min_required_mz_version,
    ) in MIN_ANCESTOR_MZ_VERSION_PER_COMMIT.items():
        if latest_published_version >= min_required_mz_version:
            continue

        if git.contains_commit(commit_hash):
            # commit would require at least min_required_mz_version
            return commit_hash

    return None


def get_latest_published_version() -> MzVersion:
    excluded_versions = set()

    while True:
        latest_published_version = git.get_latest_version(
            version_type=MzVersion, excluded_versions=excluded_versions
        )

        if image_of_release_version_exists(latest_published_version):
            return latest_published_version
        else:
            print(
                f"Skipping version {latest_published_version} (image not found), trying earlier version"
            )
            excluded_versions.add(latest_published_version)


def get_previous_published_version(release_version: MzVersion) -> MzVersion:
    excluded_versions = set()

    while True:
        previous_published_version = get_previous_mz_version(
            release_version, excluded_versions=excluded_versions
        )

        if image_of_release_version_exists(previous_published_version):
            return previous_published_version
        else:
            print(f"Skipping version {previous_published_version} (image not found)")
            excluded_versions.add(previous_published_version)


def get_published_minor_mz_versions(limit: int | None = None) -> list[MzVersion]:
    minor_mz_versions = get_all_minor_mz_versions()
    versions = []

    for v in minor_mz_versions:
        if image_of_release_version_exists(v):
            versions.append(v)

        if limit is not None and len(versions) == limit:
            break

    return versions


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
