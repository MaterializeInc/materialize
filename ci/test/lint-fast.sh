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

try bin/lint
try cargo --locked fmt -- --check
try cargo --locked deny check licenses bans sources
try cargo hakari generate --diff
try cargo hakari manage-deps --dry-run
try cargo deplint Cargo.lock ci/test/lint-deps.toml

# Smoke out failures in generating the license metadata page, even though we
# don't care about its output in the test pipeline, so that we don't only
# discover the failures after a merge to main.
try cargo --locked about generate ci/deploy/licenses.hbs > /dev/null


if [[ "${BUILDKITE_PULL_REQUEST:-true}" != "false" ]]; then
  fetch_pr_target_branch

  ci_collapsed_heading "Lint protobuf"
  # exceptions can be defined in src/buf.yaml
  COMMON_ANCESTOR="$(get_common_ancestor_commit_of_pr_and_target)"
  try buf breaking src --against ".git#ref=$COMMON_ANCESTOR,subdir=src"
fi

try_status_report
