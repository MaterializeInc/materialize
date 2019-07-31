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
    # TODO(benesch): make this easier to read.
    template="INSERT INTO slt (commit, unsupported, parse_failure, plan_failure, unexpected_plan_success, wrong_number_of_rows_inserted, inference_failure, wrong_column_names, output_failure, bail, success) VALUES ('$BUILDKITE_COMMIT', \\1, \\2, \\3, \\4, \\5, \\6, \\7, \\8, \\9, \\10);"
    sql=$(tail -n1 target/slt.out \
        | sed -E "s/^.*unsupported=([0-9]+) parse-failure=([0-9]+) plan-failure=([0-9]+) unexpected-plan-success=([0-9]+) wrong-number-of-rows-inserted=([0-9]+) inference-failure=([0-9]+) wrong-column-names=([0-9]+) output-failure=([0-9]+) bail=([0-9]+) success=([0-9]+).*$/$template/")
    ssh buildkite@mtrlz.dev psql <<< "$sql"
fi
