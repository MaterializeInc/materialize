#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

. misc/shlib/shlib.bash

MZ_REPO_REF="${BUILDKITE_REPO_REF:-origin}"
MZ_REPO_PULL_REQUEST_BASE_BRANCH="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"

if [[ "${BUILDKITE:-}" != "true" ]]; then
  # when running locally, our origin may point to a fork of the repo but we want the mz repo
  MZ_REPO_REF=$(git remote -v | grep -i "MaterializeInc/materialize" | grep "fetch" | head -n1 | cut -f1)
fi

configure_git_user_if_in_buildkite() {
  if [[ "${BUILDKITE:-}" == "true" ]]; then
    ci_collapsed_heading "Configure git"
    run git config --global user.email "buildkite@materialize.com"
    run git config --global user.name "Buildkite"
  fi
}

fetch_pr_target_branch() {
  ci_collapsed_heading "Fetch target branch"
  run git fetch "$MZ_REPO_REF" "$MZ_REPO_PULL_REQUEST_BASE_BRANCH"
}

merge_pr_target_branch() {
  configure_git_user_if_in_buildkite

  ci_collapsed_heading "Merge target branch"
  run git merge "$MZ_REPO_REF"/"$MZ_REPO_PULL_REQUEST_BASE_BRANCH" --message "Merge"
}

get_common_ancestor_commit_of_pr_and_target() {
  # Make sure we have an up to date view of main
  if git rev-parse --is-shallow-repository | grep -q true; then
    run git fetch --unshallow "$MZ_REPO_REF" "$MZ_REPO_PULL_REQUEST_BASE_BRANCH"
  else
    run git fetch "$MZ_REPO_REF" "$MZ_REPO_PULL_REQUEST_BASE_BRANCH"
  fi
  run git merge-base HEAD "$MZ_REPO_REF"/"$MZ_REPO_PULL_REQUEST_BASE_BRANCH"
}
