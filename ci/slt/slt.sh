#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# slt.sh — runs sqllogictest in CI.

set -euo pipefail

if [[ ! "${BUILDKITE-}" ]]; then
    sqllogictest() {
        cargo run --release --bin sqllogictest -- "$@"
    }
fi

export RUST_BACKTRACE=full

mkdir -p target

sqllogictest \
    -v --json-summary-file=target/slt-summary.json --no-fail "$@" \
    test/*.slt \
    test/cockroach/*.slt \
    test/sqlite/test \
    | tee target/slt.log
