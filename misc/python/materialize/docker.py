# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Docker utilities."""


from materialize import buildkite, git


def resolve_ancestor_image_tag_for_comparison() -> str:
    if buildkite.is_in_buildkite() and not buildkite.is_in_pull_request():
        latest_version = git.get_latest_version()
        print(f"Using latest version ({latest_version}) for other comparison")
        return f"v{latest_version}"
    else:
        common_ancestor_commit = buildkite.get_merge_base()
        print(f"Using merge base ({common_ancestor_commit}) for other comparison")
        return f"devel-{common_ancestor_commit}"
