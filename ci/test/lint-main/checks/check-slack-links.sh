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
# check-slack-links.sh — prevent new Slack links from being added.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash
. misc/buildkite/git.bash

CURRENT_GIT_BRANCH=$(git branch --show-current)

if [[ "${BUILDKITE_BRANCH:-}" == "main" || "$CURRENT_GIT_BRANCH" == "main" ]]; then
  echo "On main branch, skipping Slack link check."
  exit 0
fi

if [[ "${MZ_LINT_OFFLINE:-}" == 1 ]]; then
  echo "Skipping Slack link check in offline mode."
  exit 0
fi

# Use the remote-tracking ref, not FETCH_HEAD: FETCH_HEAD is a plain file that a
# concurrently-running check (check-protobuf) rewrites with its own fetch, while
# git updates remote-tracking refs atomically.
COMMON_ANCESTOR=$(git merge-base HEAD "$MZ_REPO_REF/$MZ_REPO_PULL_REQUEST_BASE_BRANCH")

# Look for added lines containing Slack links in the diff against main.
SLACK_LINES=$(git diff "$COMMON_ANCESTOR" HEAD -U0 | grep -E '^\+.*materializeinc\.slack\.com' || true)

if [[ -n "$SLACK_LINES" ]]; then
  echo "ERROR: New Slack links found in changes:"
  echo "$SLACK_LINES"
  echo
  echo "Please do not link to Slack messages, as they become inaccessible"
  echo "over time. File a GitHub issue instead and link to that."
  try false
fi

try_status_report
