# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import frontmatter

from materialize import buildkite, docker, git
from materialize.docker import (
    commit_to_image_tag,
    image_of_commit_exists,
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
    MzVersion.parse_mz("v0.81.0"),  # incompatible for upgrades
    MzVersion.parse_mz("v0.81.1"),  # incompatible for upgrades
    MzVersion.parse_mz("v0.81.2"),  # incompatible for upgrades
}

_SKIP_IMAGE_CHECK_BELOW_THIS_VERSION = MzVersion.parse_mz("v0.77.0")


"""
Git revisions that are based on commits listed as keys require at least the version specified in the value.
Note that specified versions do not necessarily need to be already published.
Commits must be ordered descending by their date.
"""
_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_AND_SCALABILITY_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#23659 (persist-txn: enable in CI with "eager uppers") introduces regressions against v0.79.0
    "c4f520a57a3046e5074939d2ea345d1c72be7079": MzVersion.parse_mz("v0.80.0"),
    # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
    "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0"),
}

"""
See: #_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_AND_SCALABILITY_REGRESSIONS
"""
_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
}

ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS = _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_AND_SCALABILITY_REGRESSIONS
ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS = _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_AND_SCALABILITY_REGRESSIONS
ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS
)


def resolve_ancestor_image_tag(ancestor_overrides: dict[str, MzVersion]) -> str:
    """
    Resolve the ancestor image tag.
    :param ancestor_overrides: one of #ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
    :return: image of the ancestor
    """
    ancestor_image_resolution = _create_ancestor_image_resolution(ancestor_overrides)
    image_tag, context = ancestor_image_resolution.resolve_image_tag()
    print(f"Using {image_tag} as image tag for ancestor (context: {context})")
    return image_tag


def _create_ancestor_image_resolution(
    ancestor_overrides: dict[str, MzVersion]
) -> AncestorImageResolutionBase:
    if buildkite.is_in_buildkite():
        return AncestorImageResolutionLocal(ancestor_overrides)
    else:
        return AncestorImageResolutionInBuildkite(ancestor_overrides)


class AncestorImageResolutionBase:
    def __init__(self, ancestor_overrides: dict[str, MzVersion]):
        self.ancestor_overrides = ancestor_overrides

    def resolve_image_tag(self) -> tuple[str, str]:
        raise NotImplementedError

    def _get_override_commit_instead_of_version(
        self,
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
        ) in self.ancestor_overrides.items():
            if latest_published_version >= min_required_mz_version:
                continue

            if git.contains_commit(commit_hash):
                # commit would require at least min_required_mz_version
                return commit_hash

        return None

    def _resolve_image_tag_of_previous_release(self, context_prefix: str):
        tagged_release_version = git.get_tagged_release_version(version_type=MzVersion)
        assert tagged_release_version is not None
        previous_release_version = get_previous_published_version(
            tagged_release_version
        )
        return (
            version_to_image_tag(previous_release_version),
            f"{context_prefix} {tagged_release_version}",
        )

    def _resolve_image_tag_of_latest_release(self, context: str):
        latest_published_version = get_latest_published_version()
        override_commit = self._get_override_commit_instead_of_version(
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
            context,
        )

    def _resolve_image_tag_of_merge_base(
        self,
        context_when_image_of_commit_exists: str,
        context_when_falling_back_to_latest: str,
    ):
        common_ancestor_commit = buildkite.get_merge_base()
        if image_of_commit_exists(common_ancestor_commit):
            return (
                commit_to_image_tag(common_ancestor_commit),
                context_when_image_of_commit_exists,
            )
        else:
            return (
                version_to_image_tag(get_latest_published_version()),
                context_when_falling_back_to_latest,
            )


class AncestorImageResolutionLocal(AncestorImageResolutionBase):
    def resolve_image_tag(self) -> tuple[str, str]:
        if git.is_on_release_version():
            return self._resolve_image_tag_of_previous_release(
                "previous release because on local release branch"
            )
        elif git.is_on_main_branch():
            return self._resolve_image_tag_of_latest_release(
                "latest release because on local main branch"
            )
        else:
            return self._resolve_image_tag_of_merge_base(
                "merge base of local non-main branch",
                "latest release because image of merge base of local non-main branch not available",
            )


class AncestorImageResolutionInBuildkite(AncestorImageResolutionBase):
    def resolve_image_tag(self) -> tuple[str, str]:
        if buildkite.is_in_pull_request():
            return self._resolve_image_tag_of_merge_base(
                "merge base of pull request",
                "latest release because image of merge base of pull request not available",
            )
        elif git.is_on_release_version():
            return self._resolve_image_tag_of_previous_release(
                "previous release because on release branch"
            )
        else:
            return self._resolve_image_tag_of_latest_release(
                "latest release because not in a pull request and not on a release branch",
            )


def get_latest_published_version() -> MzVersion:
    """Get the latest mz version for which an image is published."""
    excluded_versions = set()

    while True:
        latest_published_version = git.get_latest_version(
            version_type=MzVersion, excluded_versions=excluded_versions
        )

        if is_valid_release_image(latest_published_version):
            return latest_published_version
        else:
            print(
                f"Skipping version {latest_published_version} (image not found), trying earlier version"
            )
            excluded_versions.add(latest_published_version)


def get_previous_published_version(release_version: MzVersion) -> MzVersion:
    """Get the highest preceding mz version to the specified version for which an image is published."""
    excluded_versions = set()

    while True:
        previous_published_version = get_previous_mz_version(
            release_version, excluded_versions=excluded_versions
        )

        if is_valid_release_image(previous_published_version):
            return previous_published_version
        else:
            print(f"Skipping version {previous_published_version} (image not found)")
            excluded_versions.add(previous_published_version)


def get_published_minor_mz_versions(
    newest_first: bool = True,
    limit: int | None = None,
    include_filter: Callable[[MzVersion], bool] | None = None,
) -> list[MzVersion]:
    """
    Get the latest patch version for every minor version.
    Use this version if it is NOT important whether a tag was introduced before or after creating this branch.

    See also: #get_minor_mz_versions_listed_in_docs()
    """

    # sorted in descending order
    all_versions = get_all_mz_versions(newest_first=True)
    minor_versions: dict[str, MzVersion] = {}

    # Note that this method must not apply limit_to_published_versions to a created list
    # because in that case minor versions may get lost.
    for version in all_versions:
        if include_filter is not None and not include_filter(version):
            # this version shall not be included
            continue

        minor_version = f"{version.major}.{version.minor}"
        if minor_version in minor_versions.keys():
            # we already have a more recent version for this minor version
            continue

        if not is_valid_release_image(version):
            # this version is not considered valid
            continue

        minor_versions[minor_version] = version

        if limit is not None and len(minor_versions.keys()) == limit:
            # collected enough versions
            break

    assert len(minor_versions) > 0
    return sorted(minor_versions.values(), reverse=newest_first)


def get_minor_mz_versions_listed_in_docs() -> list[MzVersion]:
    """
    Get the latest patch version for every minor version.
    Use this version if it is important whether a tag was introduced before or after creating this branch.

    See also: #get_published_minor_mz_versions()
    """
    return VersionsFromDocs().minor_versions()


def get_all_mz_versions(
    newest_first: bool = True,
) -> list[MzVersion]:
    """
    Get all mz versions based on git tags. Versions known to be invalid are excluded.

    See also: #get_all_mz_versions_listed_in_docs
    """
    return [
        version
        for version in get_version_tags(
            version_type=MzVersion, newest_first=newest_first
        )
        if version not in INVALID_VERSIONS
    ]


def get_all_mz_versions_listed_in_docs() -> list[MzVersion]:
    """
    Get all mz versions based on docs. Versions known to be invalid are excluded.

    See also: #get_all_mz_versions()
    """
    return VersionsFromDocs().all_versions()


def get_all_published_mz_versions(
    newest_first: bool = True, limit: int | None = None
) -> list[MzVersion]:
    """Get all mz versions based on git tags. This method ensures that images of the versions exist."""
    return limit_to_published_versions(
        get_all_mz_versions(newest_first=newest_first), limit
    )


def limit_to_published_versions(
    all_versions: list[MzVersion], limit: int | None = None
) -> list[MzVersion]:
    """Remove versions for which no image is published."""
    versions = []

    for v in all_versions:
        if is_valid_release_image(v):
            versions.append(v)

        if limit is not None and len(versions) == limit:
            break

    return versions


def get_previous_mz_version(
    version: MzVersion, excluded_versions: set[MzVersion] | None = None
) -> MzVersion:
    """Get the predecessor of the specified version based on git tags."""
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


def is_valid_release_image(version: MzVersion) -> bool:
    """
    Checks if a version is not known as an invalid version and has a published image.
    Note that this method may take shortcuts on older versions.
    """
    if version in INVALID_VERSIONS:
        return False

    if version < _SKIP_IMAGE_CHECK_BELOW_THIS_VERSION:
        # optimization: assume that all versions older than this one are either valid or listed in INVALID_VERSIONS
        return True

    # This is a potentially expensive operation which pulls an image if it hasn't been pulled yet.
    return docker.image_of_release_version_exists(version)


def get_commits_of_accepted_regressions_between_versions(
    ancestor_overrides: dict[str, MzVersion],
    since_version_exclusive: MzVersion,
    to_version_inclusive: MzVersion,
) -> list[str]:
    """
    Get commits of accepted regressions between both versions.
    :param ancestor_overrides: one of #ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
    :return: commits
    """

    assert since_version_exclusive <= to_version_inclusive

    commits = []

    for (
        regression_introducing_commit,
        first_version_with_regression,
    ) in ancestor_overrides.items():
        if (
            since_version_exclusive
            < first_version_with_regression
            <= to_version_inclusive
        ):
            commits.append(regression_introducing_commit)

    return commits


class VersionsFromDocs:
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
