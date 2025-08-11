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

STEP_START_TIMESTAMP_WITH_TZ=$(date +"%Y-%m-%d %H:%M:%S%z")
export STEP_START_TIMESTAMP_WITH_TZ

ci_unimportant_heading "Download coverage data from run"
mkdir -p coverage
buildkite-agent artifact download 'coverage/*.zst' coverage/
find coverage -name '*.zst' -exec zstd -d {} \;

ci_uncollapsed_heading "Uncovered Lines in Pull Request"
find coverage -name '*.lcov' -not -name 'cargotest.lcov' -exec bin/ci-coverage-pr-report --unittests=coverage/cargotest.lcov {} +
buildkite-agent artifact upload junit_coverage*.xml
bin/ci-annotate-errors junit_coverage*.xml

ci_unimportant_heading "Create coverage report"
REPORT=coverage_without_unittests_"$BUILDKITE_BUILD_ID"
REPORT_UNITTESTS=coverage_with_unittests_"$BUILDKITE_BUILD_ID"
find coverage -name '*.lcov' -exec sed -i "s#SF:/var/lib/buildkite-agent/builds/buildkite-[^/]*/materialize/[^/]*/#SF:#" {} +
find coverage -name '*.lcov' -not -name 'cargotest.lcov' -exec genhtml -o "$REPORT" {} +
find coverage -name '*.lcov' -exec genhtml -o "$REPORT_UNITTESTS" {} +
tar -I zstd -cf "$REPORT".tar.zst "$REPORT"
tar -I zstd -cf "$REPORT_UNITTESTS".tar.zst "$REPORT_UNITTESTS"
buildkite-agent artifact upload "$REPORT".tar.zst
buildkite-agent artifact upload "$REPORT_UNITTESTS".tar.zst
