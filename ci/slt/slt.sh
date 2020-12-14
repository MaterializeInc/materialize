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
# slt.sh — runs sqllogictest in CI.

set -euo pipefail

. misc/shlib/shlib.bash

if [[ ! "${BUILDKITE-}" ]]; then
    sqllogictest() {
        cargo run --release --bin sqllogictest -- "$@"
    }
fi

if [[ "${BUILDKITE-}" ]]; then
    await_postgres -h postgres -p 5432
fi

export RUST_BACKTRACE=full

mkdir -p target

sqllogictest \
    -v --json-summary-file=target/slt-summary.json --no-fail "$@" \
    test/sqllogictest/cockroach/*.slt \
    test/sqllogictest/postgres/*.slt \
    test/sqllogictest/postgres/pgcrypto/*.slt \
    | tee target/slt.log

sqllogictest \
    -v \
    test/sqllogictest/*.slt \
    test/sqllogictest/sqlite/test \
    test/sqllogictest/transform/*.slt \
    | tee -a target/slt.log
