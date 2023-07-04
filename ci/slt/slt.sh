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

tests=(
    test/sqllogictest/cockroach/*.slt \
    test/sqllogictest/postgres/*.slt \
    test/sqllogictest/postgres/pgcrypto/*.slt \
)
tests_without_views=(
  test/sqllogictest/cockroach/tuple.slt # now()
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

sqllogictest -v --auto-index-selects --no-fail "$@" "${tests[@]}" | tee -a target/slt.log
sqllogictest -v --no-fail "$@" "${tests_without_views[@]}" | tee -a target/slt.log

tests=(
    test/sqllogictest/*.slt \
    test/sqllogictest/attributes/*.slt \
    test/sqllogictest/introspection/*.slt \
    test/sqllogictest/explain/*.slt \
    test/sqllogictest/transform/*.slt \
)
tests_without_views=(
    # errors:
    test/sqllogictest/array_fill.slt
    test/sqllogictest/as_of.slt
    test/sqllogictest/cluster.slt
    test/sqllogictest/cte_lowering.slt
    test/sqllogictest/current_database.slt
    test/sqllogictest/dates-times.slt
    test/sqllogictest/default_privileges.slt
    test/sqllogictest/disambiguate_columns.slt
    test/sqllogictest/extract.slt
    test/sqllogictest/funcs.slt
    test/sqllogictest/github-11139.slt
    test/sqllogictest/github-11568.slt
    test/sqllogictest/github-17762.slt
    test/sqllogictest/github-18522.slt
    test/sqllogictest/information_schema_columns.slt
    test/sqllogictest/information_schema_tables.slt
    test/sqllogictest/interval.slt
    test/sqllogictest/introspection/cluster_log_compaction.slt
    test/sqllogictest/introspection/replica_targeting.slt
    test/sqllogictest/joins.slt
    test/sqllogictest/list.slt
    test/sqllogictest/managed_cluster.slt
    test/sqllogictest/mz-resolve-object-name.slt
    test/sqllogictest/mztimestamp.slt
    test/sqllogictest/object_ownership.slt
    test/sqllogictest/operator.slt
    test/sqllogictest/outer_join.slt
    test/sqllogictest/parse_ident.slt
    test/sqllogictest/pg_catalog_attribute.slt
    test/sqllogictest/pg_catalog_class.slt
    test/sqllogictest/pg_catalog_matviews.slt
    test/sqllogictest/pg_catalog_namespace.slt
    test/sqllogictest/pg_catalog_proc.slt
    test/sqllogictest/pg_catalog_tablespace.slt
    test/sqllogictest/pg_catalog_views.slt
    test/sqllogictest/postgres/join-lateral.slt
    test/sqllogictest/postgres/union.slt
    test/sqllogictest/privilege_grants.slt
    test/sqllogictest/rbac_enabled.slt
    test/sqllogictest/regex.slt
    test/sqllogictest/role_membership.slt
    test/sqllogictest/schemas.slt
    test/sqllogictest/subquery.slt
    test/sqllogictest/table_func.slt
    test/sqllogictest/temporal.slt
    test/sqllogictest/timedomain.slt
    test/sqllogictest/transactions.slt
    test/sqllogictest/transform/filter_index.slt
    test/sqllogictest/transform/join_fusion.slt
    test/sqllogictest/transform/join_index.slt
    test/sqllogictest/transform/predicate_pushdown.slt
    test/sqllogictest/transform/union_cancel.slt
    test/sqllogictest/types.slt
    test/sqllogictest/vars.slt
    test/sqllogictest/web-console.slt
    test/sqllogictest/window_funcs.slt

    # different outputs:
    test/sqllogictest/audit_log.slt # seems expected for audit log to be different
    test/sqllogictest/cluster.slt # different indexes auto-created
    test/sqllogictest/interval.slt # https://github.com/MaterializeInc/materialize/issues/20110
    test/sqllogictest/operator.slt # https://github.com/MaterializeInc/materialize/issues/20110
    test/sqllogictest/temporal.slt # https://github.com/MaterializeInc/materialize/issues/20110
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
sqllogictest -v --auto-transactions --auto-index-tables --enable-table-keys "$@" test/sqllogictest/sqlite/test | tee -a target/slt.log
