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
# check-js-typedefs.sh — type-check JS files with JSDoc annotations.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if ! command -v npm &>/dev/null; then
  echo "lint: npm is not installed"
  echo "hint: install Node.js from https://nodejs.org/ or via your package manager"
fi

try npm install --prefix test/dataflow-visualizer --silent
try npx --prefix test/dataflow-visualizer tsc -p test/dataflow-visualizer/tsconfig.typecheck.json

try_status_report
