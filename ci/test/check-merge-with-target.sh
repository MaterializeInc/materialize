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
. misc/buildkite/git.bash

if git rev-parse --is-shallow-repository | grep -q true; then
  git fetch --unshallow origin "$BUILDKITE_PIPELINE_DEFAULT_BRANCH"
else
  git fetch origin "$BUILDKITE_PIPELINE_DEFAULT_BRANCH"
fi
fetch_pr_target_branch
merge_pr_target_branch

ci_collapsed_heading "Conduct checks"
bin/ci-builder run stable cargo check --all-targets
