# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Docker utilities."""


from materialize import buildkite, git

try:
    from semver.version import Version
except ImportError:
    from semver import VersionInfo as Version  # type: ignore


def resolve_ancestor_image_tag() -> str:
    image_tag, context = _resolve_ancestor_image_tag()
    print(f"Using {image_tag} as image tag for comparison (context: {context})")
    return image_tag


def _resolve_ancestor_image_tag() -> tuple[str, str]:
    if buildkite.is_in_buildkite():
        if buildkite.is_in_pull_request():
            # return the merge base
            common_ancestor_commit = buildkite.get_merge_base()
            return (
                _commit_to_image_tag(common_ancestor_commit),
                "merge base of pull request",
            )
        elif git.is_on_release_version():
            # return the previous release
            tagged_release_version = git.get_tagged_release_version()
            assert tagged_release_version is not None
            previous_release_version = git.get_previous_version(tagged_release_version)
            return (
                _mz_version_to_image_tag(previous_release_version),
                f"previous release because on release branch {tagged_release_version}",
            )
        else:
            # return the latest release
            latest_version = git.get_latest_version()
            return (
                _mz_version_to_image_tag(latest_version),
                "latest release because not in a pull request and not on a release branch",
            )
    else:
        if git.is_on_release_version():
            # return the previous release
            tagged_release_version = git.get_tagged_release_version()
            assert tagged_release_version is not None
            previous_release_version = git.get_previous_version(tagged_release_version)
            return (
                _mz_version_to_image_tag(previous_release_version),
                f"previous release because on local release branch {tagged_release_version}",
            )
        elif git.is_on_main_branch():
            # return the latest release
            latest_version = git.get_latest_version()
            return (
                _mz_version_to_image_tag(latest_version),
                "latest release because on local main branch",
            )
        else:
            # return the merge base
            common_ancestor_commit = buildkite.get_merge_base()
            return (
                _commit_to_image_tag(common_ancestor_commit),
                "merge base of local non-main branch",
            )


def _commit_to_image_tag(commit_hash: str) -> str:
    return f"devel-{commit_hash}"


def _mz_version_to_image_tag(version: Version) -> str:
    return f"v{version}"
