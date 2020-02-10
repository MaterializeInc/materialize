#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# upload.sh — uploads SQL logic test results in CI.

set -euo pipefail

set -x  # TODO(benesch): remove when this script works reliably.

if [[ "${BUILDKITE_BRANCH-}" = master && "${BUILDKITE_COMMIT-}" ]]; then
    mkdir -p target
    buildkite-agent artifact download target/slt-summary.json .
    jq --arg commit "$BUILDKITE_COMMIT" -rf ci/slt/parse-summary.jq target/slt-summary.json \
        | ssh buildkite@mtrlz.dev psql -v ON_ERROR_STOP=on
fi
