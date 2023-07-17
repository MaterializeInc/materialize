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
# slt.sh — runs sqllogictest in CI.

set -euo pipefail

mkdir -p target
rm -f target/slt.log

# All CockroachDB and PostgreSQL SLTs can be run with --auto-index-selects,
# but require --no-fail
tests=(
    test/sqllogictest/cockroach/*.slt \
    test/sqllogictest/postgres/*.slt \
    test/sqllogictest/postgres/pgcrypto/*.slt \
)

sqllogictest -v --auto-index-selects --no-fail "$@" "${tests[@]}" | tee -a target/slt.log

tests=(
    test/sqllogictest/*.slt \
    test/sqllogictest/attributes/*.slt \
    test/sqllogictest/introspection/*.slt \
    test/sqllogictest/explain/*.slt \
    test/sqllogictest/transform/*.slt \
)
tests_without_views=(
    # errors:
    test/sqllogictest/list.slt # https://github.com/MaterializeInc/materialize/issues/20534

    # transactions:
    test/sqllogictest/github-11568.slt
    test/sqllogictest/introspection/cluster_log_compaction.slt
    test/sqllogictest/timedomain.slt
    test/sqllogictest/transactions.slt

    # different outputs:
    test/sqllogictest/audit_log.slt # seems expected for audit log to be different
    test/sqllogictest/cluster.slt # different indexes auto-created
    test/sqllogictest/object_ownership.slt # different indexes auto-created
    test/sqllogictest/interval.slt # https://github.com/MaterializeInc/materialize/issues/20110
    test/sqllogictest/operator.slt # https://github.com/MaterializeInc/materialize/issues/20110
)
# Exclude tests_without_views from tests
for f in "${tests_without_views[@]}"; do
    tests=("${tests[@]/$f}")
done
# Remove empty entries from tests, since
# sqllogictests emits failures on them.
temp=()
for f in "${tests[@]}"; do
    if [ -n "$f" ]; then
        temp+=( "$f" )
    fi
done
tests=("${temp[@]}")

sqllogictest -v --auto-index-selects "$@" "${tests[@]}" | tee -a target/slt.log
sqllogictest -v "$@" "${tests_without_views[@]}" | tee -a target/slt.log

# Too slow to run with --auto-index-selects, can't run together with
# --auto-transactions, no differences seen in previous run. We might want to
# revisit and see if we can periodically test them, even if it takes 2-3 days
# for the run to finish.
find test/sqllogictest/sqlite/test -type f -print0 | xargs -0 sqllogictest -v --auto-transactions --auto-index-tables --enable-table-keys "$@" | tee -a target/slt.log
