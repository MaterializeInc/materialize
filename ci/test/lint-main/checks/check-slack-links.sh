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

if [[ "${1:-}" == --offline ]]; then
  echo "Skipping Slack link check in offline mode."
  exit 0
fi

COMMON_ANCESTOR=$(git merge-base HEAD FETCH_HEAD)

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
