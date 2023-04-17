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

ci_unimportant_heading "Download coverage data from run"
mkdir -p coverage
buildkite-agent artifact download 'coverage/*.xz' coverage/
find coverage -name '*.xz' -exec xz -d {} \;

ci_uncollapsed_heading "Uncovered Lines in Pull Request"
find coverage -name '*.lcov' -exec bin/ci-coverage-pr-report {} +
buildkite-agent artifact upload junit_coverage*.xml

ci_unimportant_heading "Create coverage report"
REPORT=coverage_"$BUILDKITE_BUILD_ID"
# This can probably be removed if I can figure out the tag issue:
find coverage -name '*.lcov' -exec sed -i "s#SF:/var/lib/buildkite-agent/builds/buildkite-.*/materialize/coverage/#SF:#" {} +
find coverage -name '*.lcov' -exec genhtml -o "$REPORT" {} +
tar cfJ "$REPORT".tar.xz "$REPORT"
buildkite-agent artifact upload "$REPORT".tar.xz
