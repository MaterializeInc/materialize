# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Git utilities."""

from materialize import spawn


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
