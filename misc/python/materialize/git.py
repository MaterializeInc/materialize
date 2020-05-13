# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Git utilities."""

from functools import lru_cache
from materialize import spawn
from pathlib import Path
from typing import Set, Union


def rev_count(rev: str) -> int:
    """Count the commits up to a revision.

    Args:
        rev: A Git revision in any format know to the Git CLI.

    Returns:
        count: The number of commits in the Git repository starting from the
            initial commit and ending with the specified commit, inclusive.
    """
    return int(
        spawn.capture(["git", "rev-list", "--count", rev, "--"], unicode=True).strip()
    )


def rev_parse(rev: str) -> str:
    """Compute the hash for a revision.

    Args:
        rev: A Git revision in any format known to the Git CLI.

    Returns:
        sha: A 40 character hex-encoded SHA-1 hash representing the ID of the
            named revision in Git's object database.
    """
    return spawn.capture(["git", "rev-parse", "--verify", rev], unicode=True).strip()


@lru_cache(maxsize=None)
def expand_globs(root: Path, *specs: Union[Path, str]) -> Set[str]:
    """Find unignored files within the specified paths."""
    # The goal here is to find all files in the working tree that are not
    # ignored by .gitignore. `git ls-files` doesn't work, because it reports
    # files that have been deleted in the working tree if they are still present
    # in the index. Using `os.walkdir` doesn't work because there is no good way
    # to evaluate .gitignore rules from Python. So we use `git diff` against the
    # empty tree, which appears to have the desired semantics.
    empty_tree = (
        "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # git hash-object -t tree /dev/null
    )
    files = spawn.capture(
        ["git", "diff", "--name-only", "-z", empty_tree, "--", *specs],
        cwd=root,
        unicode=True,
    ).split("\0")
    return set(f for f in files if f.strip() != "")
