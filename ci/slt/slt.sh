#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# slt.sh — runs sqllogictest in CI.

set -euo pipefail

verbosity=-vv
if [[ "${BUILDKITE-}" ]]; then
    verbosity=-v
fi

set -x

# TODO(jamii) We can't run test/select4.test without predicate pushdown - it's too slow
cargo run --release --bin=sqllogictest -- \
    sqllogictest/test/select1.test \
    sqllogictest/test/select2.test \
    sqllogictest/test/select3.test \
    sqllogictest/test/select5.test \
    sqllogictest/test/evidence \
    sqllogictest/test/index \
    sqllogictest/test/random \
    "$verbosity" "$@" | tee target/slt.out || true

if [[ "${BUILDKITE_BRANCH-}" = master && "${BUILDKITE_COMMIT-}" ]]; then
    template="INSERT INTO slt (commit, unsupported, parse_failure, plan_failure, inference_failure, output_failure, bail, success) VALUES ('$BUILDKITE_COMMIT', \\1, \\2, \\3, \\4, \\5, \\6, \\7);"
    sql=$(tail -n1 target/slt.out \
        | sed -E "s/^.*unsupported=([0-9]+) parse-failure=([0-9]+) plan-failure=([0-9]+) inference-failure=([0-9]+) output-failure=([0-9]+) bail=([0-9]+) success=([0-9]+).*$/$template/")
    ssh buildkite@mtrlz.dev psql <<< "$sql"
fi
