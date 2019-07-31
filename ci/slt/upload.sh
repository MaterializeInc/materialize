#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# slt.sh — uploads SQL logic test results in CI.

set -euo pipefail

set -x
if [[ "${BUILDKITE_BRANCH-}" = master && "${BUILDKITE_COMMIT-}" ]]; then
    mkdir -p target
    buildkite-agent artifact download target/slt.out target
    # TODO(benesch): handle the new fields in the output, rather than just
    # ignoring them.
    template="INSERT INTO slt (commit, unsupported, parse_failure, plan_failure, inference_failure, output_failure, bail, success) VALUES ('$BUILDKITE_COMMIT', \\1, \\2, \\3, \\4, \\5, \\6, \\7);"
    sql=$(tail -n1 target/slt.out \
        | sed -E "s/^.*unsupported=([0-9]+) parse-failure=([0-9]+) plan-failure=([0-9]+).*inference-failure=([0-9]+).*output-failure=([0-9]+) bail=([0-9]+) success=([0-9]+).*$/$template/")
    ssh buildkite@mtrlz.dev psql <<< "$sql"
fi
