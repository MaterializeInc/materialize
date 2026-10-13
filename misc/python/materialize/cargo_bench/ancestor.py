# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Clamping of the cargo-bench comparison point to the bench baseline."""

import subprocess
from pathlib import Path

from materialize import spawn

# The last commit that changed how every bench is built or measured, so that
# numbers from before and after it are not comparable. Currently the merge of
# #39364, which links every bench against the production allocator; before it
# benches ran on the system allocator.
#
# Bump this to the merge commit of any future change of that kind (allocator,
# bench profile, measurement harness). Without the bump, every comparison
# against an older ancestor reports the change itself as regressions and
# improvements until the next release branch point moves past it.
BENCH_BASELINE = "e95ca7eec04d5817669176949c48a417856cf656"


def is_ancestor(earlier: str, later: str, cwd: Path) -> bool:
    """Whether `earlier` is an ancestor of, or equal to, `later` in the local repository."""
    returncode = spawn.run_and_get_return_code(
        ["git", "merge-base", "--is-ancestor", earlier, later], cwd=cwd
    )
    # 1 means "not an ancestor"; anything else is a git error, such as a
    # commit missing from a shallow clone, which must not read as "no".
    if returncode not in (0, 1):
        raise subprocess.CalledProcessError(
            returncode, ["git", "merge-base", "--is-ancestor", earlier, later]
        )
    return returncode == 0


def clamp_to_baseline(
    ancestor: str, head: str, cwd: Path, baseline: str = BENCH_BASELINE
) -> str:
    """Return `ancestor`, or `baseline` when `head` contains `baseline` but `ancestor` does not.

    Comparing across the baseline would measure the change of bench setup
    rather than the changes in between, so the ancestor never predates it.
    A `head` that does not contain the baseline (an older branch) has the
    same setup as any ancestor and keeps its ancestor.
    """
    if is_ancestor(baseline, head, cwd) and not is_ancestor(baseline, ancestor, cwd):
        return baseline
    return ancestor
