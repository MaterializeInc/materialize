# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Benchmark utilities."""

from materialize import buildkite


def resolve_tag_of_common_ancestor(tag_when_on_default_branch: str = "latest") -> str:
    if buildkite.is_in_buildkite() and not buildkite.is_in_pull_request():
        print(f"On default branch, using {tag_when_on_default_branch} as tag")
        return tag_when_on_default_branch
    else:
        commit_hash = buildkite.get_merge_base()
        tag = f"devel-{commit_hash}"
        print(f"Resolved common-ancestor to {tag} (commit: {commit_hash})")
        return tag
