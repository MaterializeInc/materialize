#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# check-merge-with-target.sh — check if merge with target branch
# succeeds and checks still pass.

set -euo pipefail

. misc/shlib/shlib.bash

# BUILDKITE_PULL_REQUEST will either contain the number of the PR or
# the string "false".
if [[ $BUILDKITE_PULL_REQUEST == "false" ]]; then
    exit 0
fi

ci_collapsed_heading "Configure git"
BUILDKITE_REPO_REF="buildkite-origin"
run git remote add "$BUILDKITE_REPO_REF" "$BUILDKITE_REPO"
run git config --global user.email "buildkite@materialize.com"
run git config --global user.name "Buildkite"

ci_collapsed_heading "Fetch and merge target branch"
run git fetch "$BUILDKITE_REPO_REF" "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"
run git merge "$BUILDKITE_REPO_REF"/"$BUILDKITE_PULL_REQUEST_BASE_BRANCH" --message "Merge"

ci_collapsed_heading "Conduct checks"
run cargo check
