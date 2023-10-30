# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Buildkite utilities."""

import os

from materialize import git, spawn, ui


def is_in_buildkite() -> bool:
    return os.getenv("BUILDKITE", "false") == "true"


def is_in_pull_request() -> bool:
    """
    Note that this does not work in (manually triggered) nightly builds because they don't carry this information!
    Consider using #is_on_default_branch() instead.
    """
    return os.getenv("BUILDKITE_PULL_REQUEST", "false") != "false"


def is_on_default_branch() -> bool:
    current_branch = os.getenv("BUILDKITE_BRANCH", "unknown")
    default_branch = os.getenv("BUILDKITE_PIPELINE_DEFAULT_BRANCH", "main")
    return current_branch == default_branch


def get_pull_request_base_branch(fallback: str = "main"):
    return os.getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH", fallback)


def get_pipeline_default_branch(fallback: str = "main"):
    return os.getenv("BUILDKITE_PIPELINE_DEFAULT_BRANCH", fallback)


def get_remote(url: str) -> str | None:
    result = spawn.capture(["git", "remote", "--verbose"])
    for line in result.splitlines():
        remote, desc = line.split("\t")
        if desc.lower() == f"{url} (fetch)".lower():
            return remote
    return None


def get_merge_base(url: str = "https://github.com/MaterializeInc/materialize") -> str:
    # Alternative syntax
    remote = get_remote(url) or get_remote(
        url.replace("https://github.com/", "git@github.com:")
    )
    if not remote:
        print(f"Remote for URL {url} not found, using origin")
        remote = "origin"
    base_branch = get_pull_request_base_branch() or get_pipeline_default_branch()
    merge_base = git.get_common_ancestor_commit(
        remote, branch=base_branch, fetch_branch=True
    )
    return merge_base


def inline_link(url: str, label: str | None = None) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output"""
    link = f"url='{url}'"

    if label:
        link = f"{link};content='{label}'"

    # These escape codes are not supported by terminals
    return f"\033]1339;{link}\a" if ui.env_is_truthy("BUILDKITE") else f"{label},{url}"


def inline_image(url: str, alt: str) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output#images-syntax-for-inlining-images"""
    content = f"url='\"{url}\"';alt='\"{alt}\"'"
    # These escape codes are not supported by terminals
    return f"\033]1338;{content}\a" if ui.env_is_truthy("BUILDKITE") else f"{alt},{url}"
