# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Debian packaging utilities."""

from materialize import cargo, git


def unstable_version(workspace: cargo.Workspace) -> str:
    """Computes the version to use for the materialized-unstable package."""
    mz_version_string = workspace.crates["materialized"].version_string
    commit_count = git.rev_count("HEAD")
    commit_hash = git.rev_parse("HEAD")
    return f"{mz_version_string}-{commit_count}-{commit_hash}"
