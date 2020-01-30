#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
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
