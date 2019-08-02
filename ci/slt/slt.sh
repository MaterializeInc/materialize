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
else
    sqllogictest() {
        cargo run --release --bin sqllogictest -- "$@"
    }
fi

mkdir -p target

RUST_BACKTRACE=full sqllogictest \
    sqllogictest/test/ \
    test/*.slt \
    test/cockroach/*.slt \
    --json-summary-file=target/slt-summary.json \
    "$verbosity" "$@" | tee target/slt.out
