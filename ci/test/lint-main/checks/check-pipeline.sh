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
# check-pipeline.sh: Sanity check for pipelines

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

: "${CI:=0}"

if ! is_truthy "$CI"; then
    # Requires buildkite agent-access-token, which won't be available locally
    exit
fi

unset CI_TEST_IDS
unset CI_TEST_SELECTION
unset CI_SANITIZER
unset CI_COVERAGE_ENABLED
unset CI_WAITING_FOR_BUILD

pids=()
for pipeline in $(find ci -name "pipeline.template.yml" -not -path "ci/test/pipeline.template.yml" -exec dirname {} \; | cut -d/ -f2); do
    bin/pyactivate -m ci.mkpipeline "$pipeline" --dry-run &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    try wait "$pid"
done

try_status_report
