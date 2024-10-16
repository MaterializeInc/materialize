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
# check-protobuf.sh — detect breaking changes in proto files.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash
. misc/buildkite/git.bash

if ! buf --version >/dev/null 2>/dev/null; then
  echo "lint: buf is not installed"
  echo "hint: refer to https://buf.build/docs/installation for install instructions"
fi

CURRENT_GIT_BRANCH=$(try git branch --show-current)
IN_BUILDKITE=in_ci
IN_BUILDKITE_PR=0
ON_MAIN_BRANCH=0
IN_LOCAL_NON_MAIN_BRANCH=0

if [[ ${BUILDKITE_PULL_REQUEST:-false} != "false" ]]; then
  IN_BUILDKITE_PR=1
fi

if [[ "$CURRENT_GIT_BRANCH" == "main" ]]; then
  ON_MAIN_BRANCH=1
fi

if [[ "$IN_BUILDKITE" != 1 && "$ON_MAIN_BRANCH" != 1 ]]; then
  IN_LOCAL_NON_MAIN_BRANCH=1
fi

echo $IN_BUILDKITE_PR
echo $IN_LOCAL_NON_MAIN_BRANCH

if [[ "${1:-}" = --offline ]]; then
  fetch_from_git=false
else
  fetch_from_git=true
fi

if [[ $IN_BUILDKITE_PR || $IN_LOCAL_NON_MAIN_BRANCH ]]; then
  # see ./ci/test/lint-buf/README.md

  if $fetch_from_git; then
    fetch_pr_target_branch
  fi

  ci_collapsed_heading "Verify that protobuf config is up-to-date"
  try bin/pyactivate ./ci/test/lint-buf/generate-buf-config.py
  try yamllint src/buf.yaml
  try git diff --name-only --exit-code src/buf.yaml

  ci_collapsed_heading "Lint protobuf"
  COMMON_ANCESTOR="$(get_common_ancestor_commit_of_pr_and_target)"
  # Default is depth 50, which can be insufficient to grab the relevant ancestor commit
  try buf breaking src --against ".git#ref=$COMMON_ANCESTOR,subdir=src,depth=10000" --verbose

  ci_collapsed_heading "Lint protobuf formatting"
  # Proto formatting
  try buf format src --diff --exit-code
fi

try_status_report
