# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Build context providing a partial facade to git utilities and buildkite utilities."""
import os

from materialize import buildkite, git


def is_on_main_branch() -> bool:
    return get_branch_name() == "main"


def get_branch_name() -> str | None:
    if buildkite.is_in_buildkite():
        return os.getenv("BUILDKITE_BRANCH")
    else:
        return git.get_branch_name()


def is_on_release_version() -> bool:
    # this is known to work both locally and on Buildkite
    return git.is_on_release_version()
