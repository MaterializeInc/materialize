#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# upload.sh — uploads SQL logic test results in CI.

set -euo pipefail

if [[ "${BUILDKITE_BRANCH-}" = master && "${BUILDKITE_COMMIT-}" ]]; then
    buildkite-agent artifact download target/slt-summary.json .
    ssh buildkite@mtrlz.dev psql < <(jq -r parse-summary.jq slt-summary.json)
fi
