# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for release management."""

from pathlib import Path

from materialize import MZ_ROOT, spawn

PUSH_BRANCH_HELP = (
    "push the commits to this branch instead of main, for a caller that "
    "merges them into main through a pull request"
)


def doc_file_path(version: str) -> Path:
    return MZ_ROOT / "doc" / "user" / "content" / "releases" / f"{version}.md"


def push_to_main(remote: str, push_branch: str | None) -> None:
    """Pushes the checked-out main to `remote`.

    With `push_branch`, pushes HEAD to that branch instead, replacing whatever
    it held, and leaves merging it into main to the caller.
    """
    if push_branch:
        print(f"Pushing to {push_branch} on {remote}...")
        spawn.runv(["git", "push", "--force", remote, f"HEAD:refs/heads/{push_branch}"])
    else:
        print(f"Pushing to {remote}...")
        spawn.runv(["git", "push", remote, "main"])
