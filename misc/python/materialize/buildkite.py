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
from pathlib import Path

from materialize import git, spawn, ui


def is_in_buildkite() -> bool:
    return ui.env_is_truthy("BUILDKITE")


def is_in_pull_request() -> bool:
    """Note that this is a heuristic."""

    if not is_in_buildkite():
        return False

    if is_pull_request_marker_set():
        return True

    if is_on_default_branch():
        return False

    is_tagged_commit = len(git.get_tags_of_current_commit()) > 0
    if is_tagged_commit:
        # likely a release version
        return False

    return True


def is_pull_request_marker_set() -> bool:
    return ui.env_is_truthy("BUILDKITE_PULL_REQUEST")


def is_on_default_branch() -> bool:
    current_branch = os.getenv("BUILDKITE_BRANCH", "unknown")
    default_branch = os.getenv("BUILDKITE_PIPELINE_DEFAULT_BRANCH", "main")
    return current_branch == default_branch


def get_pull_request_base_branch(fallback: str = "main"):
    return os.getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH", fallback)


def get_pipeline_default_branch(fallback: str = "main"):
    return os.getenv("BUILDKITE_PIPELINE_DEFAULT_BRANCH", fallback)


def get_merge_base(url: str = "https://github.com/MaterializeInc/materialize") -> str:
    base_branch = get_pull_request_base_branch() or get_pipeline_default_branch()
    merge_base = git.get_common_ancestor_commit(
        remote=git.get_remote(url), branch=base_branch, fetch_branch=True
    )
    return merge_base


def inline_link(url: str, label: str | None = None) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output"""
    link = f"url='{url}'"

    if label:
        link = f"{link};content='{label}'"

    # These escape codes are not supported by terminals
    return f"\033]1339;{link}\a" if is_in_buildkite() else f"{label},{url}"


def inline_image(url: str, alt: str) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output#images-syntax-for-inlining-images"""
    content = f"url='\"{url}\"';alt='\"{alt}\"'"
    # These escape codes are not supported by terminals
    return f"\033]1338;{content}\a" if is_in_buildkite() else f"{alt},{url}"


def find_modified_lines() -> set[tuple[str, int]]:
    """
    Find each line that has been added or modified in the current pull request.
    """
    merge_base = get_merge_base()
    print(f"Merge base: {merge_base}")
    result = spawn.capture(["git", "diff", "-U0", merge_base])

    modified_lines: set[tuple[str, int]] = set()
    file_path = None
    for line in result.splitlines():
        # +++ b/src/adapter/src/coord/command_handler.rs
        if line.startswith("+++"):
            file_path = line.removeprefix("+++ b/")
        # @@ -641,7 +640,6 @@ impl Coordinator {
        elif line.startswith("@@ "):
            # We only care about the second value ("+640,6" in the example),
            # which contains the line number and length of the modified block
            # in new code state.
            parts = line.split(" ")[2]
            if "," in parts:
                start, length = map(int, parts.split(","))
            else:
                start = int(parts)
                length = 1
            for line_nr in range(start, start + length):
                assert file_path
                modified_lines.add((file_path, line_nr))
    return modified_lines


def upload_artifact(path: Path | str, cwd: Path | None = None):
    spawn.runv(
        [
            "buildkite-agent",
            "artifact",
            "upload",
            path,
        ],
        cwd=cwd,
    )
