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
# check-zizmor.sh - Static analysis for GitHub Actions

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if ! zizmor --version >/dev/null 2>/dev/null; then
  echo "lint: zizmor is not installed"
  echo "hint: Install with \`cargo install --locked zizmor@1.18.0\`"
  exit 1
fi

try zizmor --offline .github

try_status_report
