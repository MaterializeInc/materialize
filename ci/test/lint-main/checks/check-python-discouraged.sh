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
# check-python-discouraged.sh — check discouraged commands.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_PYTHON:-}" ]]; then
  # check whether occurrences of `run("testdrive"` (in a single line or spread across two lines) exist, which are discouraged

  # shellcheck disable=SC2016
  SINGLE_LINE_MATCHES=$(find misc/python/materialize test -name '*.py' -print0 | xargs -0 awk '/\.run\("testdrive",/ {print FILENAME ":" FNR} {prev_line = $0}')
  # shellcheck disable=SC2016
  MULTI_LINE_MATCHES=$(find misc/python/materialize test -name '*.py' -print0 | xargs -0 awk '/"testdrive",/ && prev_line ~ /\.run\($/ {print FILENAME ":" FNR} {prev_line = $0}')

  MULTI_LINE_MATCHES=$(echo "$MULTI_LINE_MATCHES" | grep -v "misc/python/materialize/mzcompose/composition.py" || true)

  if [ -n "$SINGLE_LINE_MATCHES" ] || [ -n "$MULTI_LINE_MATCHES" ]; then
      echo "Use \`.run_testdrive_files(\` instead of \`.run(\"testdrive\"\`:"

      if [ -n "$SINGLE_LINE_MATCHES" ]; then
          echo "$SINGLE_LINE_MATCHES"
      fi
      if [ -n "$MULTI_LINE_MATCHES" ]; then
          echo "$MULTI_LINE_MATCHES"
      fi

      exit 1
  fi

fi

try_status_report
