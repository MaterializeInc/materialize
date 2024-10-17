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
# check-bazel.sh — check for Bazel issues (e.g., ensure all of our generated files are up to date).

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_BAZEL_CHECK:-}" ]]; then
  if try bin/bazel gen; then
      :
  else
      if [ $? -eq 32 ]; then
          # Network problems, retry in a bit
          sleep 20
          try bin/bazel gen
      fi
  fi

  # Make sure we didn't generate any changes.
  try git diff --compact-summary --exit-code -- '*/BUILD.bazel'
  if try_last_failed; then
      echo "lint: $(red error:) discrepancies found in generated 'BUILD.bazel' files"
      echo "lint: $(green hint:) run $(white bin/bazel gen)" >&2
  fi
fi

try_status_report
