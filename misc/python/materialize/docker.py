# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Docker utilities."""
import subprocess

from materialize import buildkite, git
from materialize.mz_version import MzVersion

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
        if _image_of_commit_exists(common_ancestor_commit):
            return (
                _commit_to_image_tag(common_ancestor_commit),
                "merge base of pull request",
            )
        else:
            return (
                _version_to_image_tag(get_latest_published_version()),
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
            _version_to_image_tag(previous_release_version),
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
                _commit_to_image_tag(override_commit),
                f"commit override instead of latest release ({latest_published_version})",
            )

        # return the latest release
        return (
            _version_to_image_tag(latest_published_version),
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
            _version_to_image_tag(previous_release_version),
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
                _commit_to_image_tag(override_commit),
                f"commit override instead of latest release ({latest_published_version})",
            )

        return (
            _version_to_image_tag(latest_published_version),
            "latest release because on local main branch",
        )
    else:
        # return the merge base
        common_ancestor_commit = buildkite.get_merge_base()
        if _image_of_commit_exists(common_ancestor_commit):
            return (
                _commit_to_image_tag(common_ancestor_commit),
                "merge base of local non-main branch",
            )
        else:
            return (
                _version_to_image_tag(get_latest_published_version()),
                "latest release because image of merge base of local non-main branch not available",
            )


def get_latest_published_version() -> MzVersion:
    excluded_versions = set()

    while True:
        latest_published_version = git.get_latest_version(
            version_type=MzVersion, excluded_versions=excluded_versions
        )

        if _image_of_release_version_exists(latest_published_version):
            return latest_published_version
        else:
            print(
                f"Skipping version {latest_published_version} (image not found), trying earlier version"
            )
            excluded_versions.add(latest_published_version)


def get_previous_published_version(release_version: MzVersion) -> MzVersion:
    excluded_versions = set()

    while True:
        previous_published_version = git.get_previous_version(
            release_version, excluded_versions=excluded_versions
        )

        if _image_of_release_version_exists(previous_published_version):
            return previous_published_version
        else:
            print(f"Skipping version {previous_published_version} (image not found)")
            excluded_versions.add(previous_published_version)


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


def _image_of_release_version_exists(version: MzVersion) -> bool:
    return _mz_image_tag_exists(_version_to_image_tag(version))


def _image_of_commit_exists(commit_hash: str) -> bool:
    return _mz_image_tag_exists(_commit_to_image_tag(commit_hash))


def _mz_image_tag_exists(image_tag: str) -> bool:
    image = f"materialize/materialized:{image_tag}"
    command = [
        "docker",
        "pull",
        image,
    ]

    print(f"Trying to pull image: {image}")

    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT, text=True)
        return True
    except subprocess.CalledProcessError as e:
        return "not found: manifest unknown: manifest unknown" not in e.output


def _commit_to_image_tag(commit_hash: str) -> str:
    return f"devel-{commit_hash}"


def _version_to_image_tag(version: MzVersion) -> str:
    return str(version)
