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
# docs-pre-pr-review-claude.sh — calls claude to review the documentation
# changes using the .prompts/docs_pre_pr_review.md prompt.
#
# Usage: docs-pre-pr-review-claude.sh [--range <git-rangeA..git-rangeB>] [--staged] [--worktree] [files...]
#

set -euo pipefail

PROMPT_FILE=".prompts/docs_pre_pr_review.md"

usage() {
  echo "Usage:"
  echo "  docs-pre-pr-review-claude.sh --range <git-rangeA..git-rangeB>"
  echo "  docs-pre-pr-review-claude.sh --staged"
  echo "  docs-pre-pr-review-claude.sh --worktree"
  echo "  docs-pre-pr-review-claude.sh <files...>"
  exit 1
}

[[ $# -eq 0 ]] && usage

case "$1" in
  --range)
    [[ $# -ne 2 ]] && usage
    EXTRA="Review changes in range: $2"
    ;;
  --staged)
    [[ $# -ne 1 ]] && usage
    EXTRA="Review staged changes."
    ;;
  --worktree)
    [[ $# -ne 1 ]] && usage
    EXTRA="Review unstaged working tree changes."
    ;;
  *)
    EXTRA="Review only the following files:"
    for f in "$@"; do
      EXTRA="$EXTRA\n- $f"
    done
    ;;
esac

{
  cat "$PROMPT_FILE"
  echo ""
  echo "----"
  echo "$EXTRA"
} | claude
