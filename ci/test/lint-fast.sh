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
# lint-fast.sh — fast linters that don't require building any code.

set -euo pipefail

. misc/shlib/shlib.bash
. misc/buildkite/git.bash

ci_try bin/lint
ci_try cargo --locked fmt -- --check
ci_try cargo --locked deny check licenses bans sources
ci_try cargo hakari generate --diff
ci_try cargo hakari manage-deps --dry-run

# Smoke out failures in generating the license metadata page, even though we
# don't care about its output in the test pipeline, so that we don't only
# discover the failures after a merge to main.
ci_try cargo --locked about generate ci/deploy/licenses.hbs > /dev/null


if [[ "$BUILDKITE_PULL_REQUEST" != "false" ]]; then
  fetch_pr_target_branch

  ci_collapsed_heading "Lint protobuf"
  COMMON_ANCESTOR="$(run git merge-base HEAD "$BUILDKITE_REPO_REF"/"$BUILDKITE_PULL_REQUEST_BASE_BRANCH")"
  ci_try buf breaking src --against ".git#ref=$COMMON_ANCESTOR,subdir=src"
fi

ci_status_report
