# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

import datetime
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import frontmatter
import requests
import yaml

from materialize import build_context, buildkite, docker, git
from materialize.docker import (
    commit_to_image_tag,
    image_of_commit_exists,
    release_version_to_image_tag,
)
from materialize.git import get_version_tags
from materialize.mz_version import MzVersion

MZ_ROOT = Path(os.environ["MZ_ROOT"])


@dataclass
class SelfManagedVersion:
    helm_version: MzVersion
    version: MzVersion


def fetch_self_managed_versions() -> list[SelfManagedVersion]:
    result: list[SelfManagedVersion] = []
    for entry in yaml.safe_load(
        requests.get("https://materializeinc.github.io/materialize/index.yaml").text
    )["entries"]["materialize-operator"]:
        self_managed_version = SelfManagedVersion(
            MzVersion.parse_mz(entry["version"]),
            MzVersion.parse_mz(entry["appVersion"]),
        )
        if (
            not self_managed_version.version.prerelease
            and self_managed_version.version not in BAD_SELF_MANAGED_VERSIONS
        ):
            result.append(self_managed_version)
    return result


def get_all_self_managed_versions() -> list[MzVersion]:
    return sorted([version.version for version in fetch_self_managed_versions()])


def get_self_managed_versions() -> list[MzVersion]:
    prefixes = set()
    result = set()
    self_managed_versions = fetch_self_managed_versions()
    for version_info in self_managed_versions:
        prefix = (version_info.version.major, version_info.version.minor)
        if (
            not version_info.version.prerelease
            and prefix not in prefixes
            and not version_info.helm_version.prerelease
        ):
            result.add(version_info.version)
            prefixes.add(prefix)
    return sorted(result)


# Gets the range of versions we can "upgrade from" to the current version, sorted in ascending order.
def get_compatible_upgrade_from_versions() -> list[MzVersion]:

    # Determine the current MzVersion from the environment, or from a version constant
    current_version = MzVersion.parse_cargo()

    published_versions_within_one_major_version = {
        v
        for v in get_published_mz_versions_within_one_major_version()
        if abs(v.major - current_version.major) <= 1 and v <= current_version
    }

    if current_version.major <= 26:
        # For versions <= 26, we can only upgrade from 25.2 self-managed versions
        self_managed_25_2_versions = {
            v.version
            for v in fetch_self_managed_versions()
            if v.helm_version.major == 25 and v.helm_version.minor == 2
        }

        return sorted(
            self_managed_25_2_versions.union(
                published_versions_within_one_major_version
            )
        )
    else:
        # For versions > 26, get all mz versions within 1 major version of current_version
        return sorted(published_versions_within_one_major_version)


BAD_SELF_MANAGED_VERSIONS = {
    MzVersion.parse_mz("v0.130.0"),
    MzVersion.parse_mz("v0.130.1"),
    MzVersion.parse_mz("v0.130.2"),
    MzVersion.parse_mz("v0.130.3"),
    MzVersion.parse_mz("v0.130.4"),
    MzVersion.parse_mz(
        "v0.147.7"
    ),  # Incompatible for upgrades because it clears login attribute for roles due to catalog migration
    MzVersion.parse_mz(
        "v0.147.14"
    ),  # Incompatible for upgrades because it clears login attribute for roles due to catalog migration
    MzVersion.parse_mz("v0.157.0"),
}

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
    MzVersion.parse_mz("v0.89.7"),
    MzVersion.parse_mz("v0.92.0"),  # incompatible for upgrades
    MzVersion.parse_mz("v0.93.0"),  # accidental release
    MzVersion.parse_mz("v0.99.1"),  # incompatible for upgrades
    MzVersion.parse_mz("v0.113.1"),  # incompatible for upgrades
}

_SKIP_IMAGE_CHECK_BELOW_THIS_VERSION = MzVersion.parse_mz("v0.77.0")


def resolve_ancestor_image_tag(ancestor_overrides: dict[str, MzVersion]) -> str | None:
    """
    Resolve the ancestor image tag.
    :param ancestor_overrides: one of #ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS, #ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
    :return: image of the ancestor
    """

    manual_ancestor_override = os.getenv("COMMON_ANCESTOR_OVERRIDE")
    if manual_ancestor_override is not None:
        image_tag = _manual_ancestor_specification_to_image_tag(
            manual_ancestor_override
        )
        print(
            f"Using specified {image_tag} as image tag for ancestor (context: specified in $COMMON_ANCESTOR_OVERRIDE)"
        )
        return image_tag

    ancestor_image_resolution = _create_ancestor_image_resolution(ancestor_overrides)
    result = ancestor_image_resolution.resolve_image_tag()
    if result is None:
        return None
    image_tag, context = result
    print(f"Using {image_tag} as image tag for ancestor (context: {context})")
    return image_tag


def _create_ancestor_image_resolution(
    ancestor_overrides: dict[str, MzVersion],
) -> AncestorImageResolutionBase:
    if buildkite.is_in_buildkite():
        return AncestorImageResolutionInBuildkite(ancestor_overrides)
    else:
        return AncestorImageResolutionLocal(ancestor_overrides)


def _manual_ancestor_specification_to_image_tag(ancestor_spec: str) -> str:
    if MzVersion.is_valid_version_string(ancestor_spec):
        return release_version_to_image_tag(MzVersion.parse_mz(ancestor_spec))
    else:
        return commit_to_image_tag(ancestor_spec)


class AncestorImageResolutionBase:
    def __init__(self, ancestor_overrides: dict[str, MzVersion]):
        self.ancestor_overrides = ancestor_overrides

    def resolve_image_tag(self) -> tuple[str, str]:
        raise NotImplementedError

    def _get_override_commit_instead_of_version(
        self,
        version: MzVersion,
    ) -> str | None:
        """
        If a commit specifies a mz version as prerequisite (to avoid regressions) that is newer than the provided
        version (i.e., prerequisite not satisfied by the latest version), then return that commit's hash if the commit
        contained in the current state.
        Otherwise, return none.
        """
        for (
            commit_hash,
            min_required_mz_version,
        ) in self.ancestor_overrides.items():
            if version >= min_required_mz_version:
                continue

            if git.contains_commit(commit_hash):
                # commit would require at least min_required_mz_version
                return commit_hash

        return None

    def _resolve_image_tag_of_previous_release(
        self, context_prefix: str, previous_minor: bool
    ) -> tuple[str, str] | None:
        tagged_release_version = git.get_tagged_release_version(version_type=MzVersion)
        assert tagged_release_version is not None
        previous_release_version = get_previous_published_version(
            tagged_release_version, previous_minor=previous_minor
        )

        override_commit = self._get_override_commit_instead_of_version(
            previous_release_version
        )

        if override_commit is not None:
            # TODO(def-): This currently doesn't work because we only tag the Optimized builds with tags like v0.164.0-dev.0--main.gc28d0061a6c9e63ee50a5f555c5d90373d006686, but not the Release builds we should use
            # use the commit instead of the previous release
            # return (
            #     commit_to_image_tag(override_commit),
            #     f"commit override instead of previous release ({previous_release_version})",
            # )
            return None

        return (
            release_version_to_image_tag(previous_release_version),
            f"{context_prefix} {tagged_release_version}",
        )

    def _resolve_image_tag_of_previous_release_from_current(
        self, context: str
    ) -> tuple[str, str] | None:
        # Even though we are on main we might be in an older state, pick the
        # latest release that was before our current version.
        current_version = MzVersion.parse_cargo()

        previous_published_version = get_previous_published_version(
            current_version, previous_minor=True
        )
        override_commit = self._get_override_commit_instead_of_version(
            previous_published_version
        )

        if override_commit is not None:
            # TODO(def-): This currently doesn't work because we only tag the Optimized builds with tags like v0.164.0-dev.0--main.gc28d0061a6c9e63ee50a5f555c5d90373d006686, but not the Release builds we should use
            # use the commit instead of the latest release
            # return (
            #     commit_to_image_tag(override_commit),
            #     f"commit override instead of latest release ({previous_published_version})",
            # )
            return None

        return (
            release_version_to_image_tag(previous_published_version),
            context,
        )

    def _resolve_image_tag_of_merge_base(
        self,
        context_when_image_of_commit_exists: str,
        context_when_falling_back_to_latest: str,
    ) -> tuple[str, str] | None:
        # If the current PR has a known and accepted regression, don't compare
        # against merge base of it
        override_commit = self._get_override_commit_instead_of_version(
            MzVersion.parse_cargo()
        )
        common_ancestor_commit = buildkite.get_merge_base()

        if override_commit is not None:
            # TODO(def-): This currently doesn't work because we only tag the Optimized builds with tags like v0.164.0-dev.0--main.gc28d0061a6c9e63ee50a5f555c5d90373d006686, but not the Release builds we should use
            # return (
            #     commit_to_image_tag(override_commit),
            #     f"commit override instead of merge base ({common_ancestor_commit})",
            # )
            return None

        if image_of_commit_exists(common_ancestor_commit):
            return (
                commit_to_image_tag(common_ancestor_commit),
                context_when_image_of_commit_exists,
            )
        else:
            return (
                release_version_to_image_tag(get_latest_published_version()),
                context_when_falling_back_to_latest,
            )


class AncestorImageResolutionLocal(AncestorImageResolutionBase):
    def resolve_image_tag(self) -> tuple[str, str] | None:
        if build_context.is_on_release_version():
            return self._resolve_image_tag_of_previous_release(
                "previous minor release because on local release branch",
                previous_minor=True,
            )
        elif build_context.is_on_main_branch():
            return self._resolve_image_tag_of_previous_release_from_current(
                "previous release from current because on local main branch"
            )
        else:
            return self._resolve_image_tag_of_merge_base(
                "merge base of local non-main branch",
                "latest release because image of merge base of local non-main branch not available",
            )


class AncestorImageResolutionInBuildkite(AncestorImageResolutionBase):
    def resolve_image_tag(self) -> tuple[str, str] | None:
        if buildkite.is_in_pull_request():
            return self._resolve_image_tag_of_merge_base(
                "merge base of pull request",
                "latest release because image of merge base of pull request not available",
            )
        elif build_context.is_on_release_version():
            return self._resolve_image_tag_of_previous_release(
                "previous minor release because on release branch", previous_minor=True
            )
        else:
            return self._resolve_image_tag_of_previous_release_from_current(
                "previous release from current because not in a pull request and not on a release branch",
            )


def get_latest_published_version() -> MzVersion:
    """Get the latest mz version, older than current state, for which an image is published."""
    excluded_versions = set()
    current_version = MzVersion.parse_cargo()

    while True:
        latest_published_version = git.get_latest_version(
            version_type=MzVersion,
            excluded_versions=excluded_versions,
            current_version=current_version,
        )

        if is_valid_release_image(latest_published_version):
            return latest_published_version
        else:
            print(
                f"Skipping version {latest_published_version} (image not found), trying earlier version"
            )
            excluded_versions.add(latest_published_version)


def get_previous_published_version(
    release_version: MzVersion, previous_minor: bool
) -> MzVersion:
    """Get the highest preceding mz version to the specified version for which an image is published."""
    excluded_versions = set()

    while True:
        previous_published_version = get_previous_mz_version(
            release_version,
            previous_minor=previous_minor,
            excluded_versions=excluded_versions,
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
    exclude_current_minor_version: bool = False,
) -> list[MzVersion]:
    """
    Get the latest patch version for every minor version.
    Use this version if it is NOT important whether a tag was introduced before or after creating this branch.

    See also: #get_minor_mz_versions_listed_in_docs()
    """

    # sorted in descending order
    all_versions = get_all_mz_versions(newest_first=True)
    minor_versions: dict[str, MzVersion] = {}

    version = MzVersion.parse_cargo()
    current_version = f"{version.major}.{version.minor}"

    # Note that this method must not apply limit_to_published_versions to a created list
    # because in that case minor versions may get lost.
    for version in all_versions:
        if include_filter is not None and not include_filter(version):
            # this version shall not be included
            continue

        minor_version = f"{version.major}.{version.minor}"

        if exclude_current_minor_version and minor_version == current_version:
            continue

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


def get_minor_mz_versions_listed_in_docs(respect_released_tag: bool) -> list[MzVersion]:
    """
    Get the latest patch version for every minor version in ascending order.
    Use this version if it is important whether a tag was introduced before or after creating this branch.

    See also: #get_published_minor_mz_versions()
    """
    return VersionsFromDocs(respect_released_tag).minor_versions()


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
        # Exclude release candidates
        and not version.prerelease
    ]


def get_all_mz_versions_listed_in_docs(
    respect_released_tag: bool,
) -> list[MzVersion]:
    """
    Get all mz versions based on docs. Versions known to be invalid are excluded.

    See also: #get_all_mz_versions()
    """
    return VersionsFromDocs(respect_released_tag).all_versions()


def get_all_published_mz_versions(
    newest_first: bool = True, limit: int | None = None
) -> list[MzVersion]:
    """Get all mz versions based on git tags. This method ensures that images of the versions exist."""
    all_versions = get_all_mz_versions(newest_first=newest_first)
    print(f"all_versions: {all_versions}")
    return limit_to_published_versions(all_versions, limit)


def get_published_mz_versions_within_one_major_version(
    newest_first: bool = True,
) -> list[MzVersion]:
    """Get all previous mz versions within one major version of the current version. Ensure that images of the versions exist."""
    current_version = MzVersion.parse_cargo()
    all_versions = get_all_mz_versions(newest_first=newest_first)
    versions_within_one_major_version = {
        v
        for v in all_versions
        if abs(v.major - current_version.major) <= 1 and v <= current_version
    }

    return limit_to_published_versions(list(versions_within_one_major_version))


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
    version: MzVersion,
    previous_minor: bool,
    excluded_versions: set[MzVersion] | None = None,
) -> MzVersion:
    """Get the predecessor of the specified version based on git tags."""
    if excluded_versions is None:
        excluded_versions = set()

    if previous_minor:
        version = MzVersion.create(version.major, version.minor, 0)

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
    """Materialize versions as listed in doc/user/content/releases

    >>> len(VersionsFromDocs(respect_released_tag=True).all_versions()) > 0
    True

    >>> len(VersionsFromDocs(respect_released_tag=True).minor_versions()) > 0
    True

    >>> len(VersionsFromDocs(respect_released_tag=True).patch_versions(minor_version=MzVersion.parse_mz("v0.52.0")))
    4

    >>> min(VersionsFromDocs(respect_released_tag=True).all_versions())
    MzVersion(major=0, minor=27, patch=0, prerelease=None, build=None)
    """

    def __init__(
        self,
        respect_released_tag: bool,
        respect_date: bool = False,
        only_publish_helm_chart: bool = True,
    ) -> None:
        files = Path(MZ_ROOT / "doc" / "user" / "content" / "releases").glob("v*.md")
        self.versions = []
        current_version = MzVersion.parse_cargo()
        for f in files:
            base = f.stem
            metadata = frontmatter.load(f)
            if respect_released_tag and not metadata.get("released", False):
                continue
            if only_publish_helm_chart and not metadata.get("publish_helm_chart", True):
                continue
            date: datetime.date = metadata["date"]
            if respect_date and date > datetime.date.today():
                continue

            current_patch = metadata.get("patch", 0)
            current_rc = metadata.get("rc", 0)

            if current_rc > 0:
                for rc in range(1, current_rc + 1):
                    version = MzVersion.parse_mz(f"{base}.{current_patch}-rc.{rc}")
                    if not respect_released_tag and version >= current_version:
                        continue
                    if version not in INVALID_VERSIONS:
                        self.versions.append(version)
            else:
                for patch in range(current_patch + 1):
                    version = MzVersion.parse_mz(f"{base}.{patch}")
                    if not respect_released_tag and version >= current_version:
                        continue
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
