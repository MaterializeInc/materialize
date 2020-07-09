# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Git utilities."""

import sys
from functools import lru_cache, total_ordering
from pathlib import Path
from typing import List, Optional, Set, Union, NamedTuple

from materialize import spawn


class Tag(NamedTuple):
    major: int
    minor: int
    micro: int
    patch: Optional[str]

    @staticmethod
    def from_str(tag: str) -> "Tag":
        """Parse a single line of the output of `git tag`

        Raises `ValueError` if the tag does not look right
        """
        try:
            major, minor, micro = tag.lstrip("v").split(".")
        except ValueError:
            raise ValueError(f"Invalid tag: {tag!r}")
        patch = None
        if "-" in micro:
            micro, patch = micro.split("-")
        return Tag(int(major), int(minor), int(micro), patch)

    def is_release(self) -> bool:
        """True if this is not a pre-release, False otherwise """
        return self.patch is not None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Tag):
            return False
        return (self.major, self.minor, self.micro, self.patch) == (
            other.major,
            other.minor,
            other.micro,
            other.patch,
        )

    # this, and the below type: ignore, are because lt is defined as accepting any
    # `tuple`, but we will (a) never pass in a regular tuple, (b) don't want to allow
    # passing in a regular tuple, (c) don't want to make the code even more ugly to
    # support it
    def __lt__(self, other: "Tag") -> bool:  # type: ignore
        """Comparison where patch versions compare less than None, all else being equal
        """
        version_lt = (self.major, self.minor, self.micro) < (
            other.major,
            other.minor,
            other.micro,
        )

        if self.patch is None and other.patch is None:
            return version_lt
        elif self.patch is not None and other.patch is not None:
            return version_lt or self.patch < other.patch
        if version_lt:
            return True

        version_eq = (self.major, self.minor, self.micro) == (
            other.major,
            other.minor,
            other.micro,
        )
        if not version_eq:
            return False

        # If equal, the one that is not a patch is the release version, which is greater
        # than the patch version
        return self.patch is not None and other.patch is None

    def __gt__(self, other: "Tag") -> bool:  # type: ignore
        return not self < other and not self == other


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


def get_version_tags(*, fetch: bool = True) -> List[Tag]:
    """List all the version-like tags in the repo

    Args:
        fetch: If false, don't update git, only intended for testing
    """
    if fetch:
        spawn.runv(["git", "fetch", "--tags"])
    tags = []
    for t in spawn.capture(["git", "tag"], unicode=True).splitlines():
        try:
            tags.append(Tag.from_str(t))
        except ValueError as e:
            print(f"WARN: {e}", file=sys.stderr)

    return sorted(tags, reverse=True)
