#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -uo pipefail

cd /workdir

echo "Installing npm dependencies..."
npm cache clean --force 2>/dev/null || true
npm install

echo "Running Playwright tests..."
TEST_EXIT_CODE=0
npx playwright test || TEST_EXIT_CODE=$?

# If tests failed and we have test results with traces, package them for artifact upload
if [[ $TEST_EXIT_CODE -ne 0 && -d test-results ]]; then
    echo "Tests failed. Packaging Playwright traces for artifact upload..."
    tar -czf playwright-traces.tar.gz test-results/
    echo "Traces packaged in playwright-traces.tar.gz"
fi

exit $TEST_EXIT_CODE