#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# test.sh — runs tests and lints in CI.

set -euo pipefail

die() {
    echo "$@" >&2
    exit 1
}

fast=
for arg
do
    case "$arg" in
        --fast) fast=1 ;;
        --fast=*) die "--fast option does not take an argument" ;;
        -*) die "unknown option $arg" ;;
        *) die "usage: $0 [--fast]" ;;
    esac
done

passed=0
total=0

try() {
    # Start a collapsed-by-default log section.
    echo "--- $@"

    # Try the command.
    if "$@"; then
        ((++passed))
    else
        # The command failed. Tell Buildkite to uncollapse this log section, so
        # that the errors are immediately visible.
        [[ "${BUILDKITE-}" ]] && echo "^^^ +++"
    fi
    ((++total))
}

# A subset of SQL logic tests that can be run relatively quickly while still
# providing relatively full coverage. Note that for a file to be included here
# every query in it must be successful.
sqllogictests=(
    # sqllogictest/test/evidence/in1.test
    # sqllogictest/test/evidence/in2.test
    sqllogictest/test/evidence/slt_lang_aggfunc.test
    sqllogictest/test/evidence/slt_lang_createtrigger.test
    sqllogictest/test/evidence/slt_lang_createview.test
    sqllogictest/test/evidence/slt_lang_dropindex.test
    sqllogictest/test/evidence/slt_lang_droptable.test
    sqllogictest/test/evidence/slt_lang_droptrigger.test
    sqllogictest/test/evidence/slt_lang_dropview.test
    sqllogictest/test/evidence/slt_lang_reindex.test
    sqllogictest/test/evidence/slt_lang_replace.test
    # sqllogictest/test/evidence/slt_lang_update.test
    # sqllogictest/test/index/between/1/slt_good_0.test
    # sqllogictest/test/index/commute/10/slt_good_0.test
    # sqllogictest/test/index/delete/1/slt_good_0.test
    # sqllogictest/test/index/in/10/slt_good_0.test
    # sqllogictest/test/index/orderby_nosort/10/slt_good_0.test
    # sqllogictest/test/index/orderby/10/slt_good_0.test
    sqllogictest/test/index/random/10/slt_good_0.test
    sqllogictest/test/index/view/10/slt_good_0.test
    sqllogictest/test/random/aggregates/slt_good_0.test
    sqllogictest/test/random/expr/slt_good_0.test
    sqllogictest/test/random/groupby/slt_good_0.test
    sqllogictest/test/random/select/slt_good_0.test
    # sqllogictest/test/select1.test
    # sqllogictest/test/select2.test
    # sqllogictest/test/select3.test
    # sqllogictest/test/select4.test
    sqllogictest/test/select5.test
    test/*.slt
    # test/cockroach/aggregate.slt
    test/cockroach/alias_types.slt
    test/cockroach/alter_column_type.slt
    test/cockroach/alter_table.slt
    # test/cockroach/apply_join.slt
    test/cockroach/array.slt
    test/cockroach/as_of.slt
    test/cockroach/bit.slt
    # test/cockroach/builtin_function.slt
    test/cockroach/bytes.slt
    # test/cockroach/case_sensitive_names.slt
    test/cockroach/collatedstring.slt
    # test/cockroach/collatedstring_constraint.slt
    test/cockroach/collatedstring_index1.slt
    test/cockroach/collatedstring_index2.slt
    test/cockroach/collatedstring_normalization.slt
    test/cockroach/collatedstring_nullinindex.slt
    test/cockroach/collatedstring_uniqueindex1.slt
    test/cockroach/collatedstring_uniqueindex2.slt
    test/cockroach/computed.slt
    # test/cockroach/conditional.slt
    test/cockroach/create_as.slt
    test/cockroach/custom_escape_character.slt
    test/cockroach/database.slt
    # test/cockroach/datetime.slt
    # test/cockroach/decimal.slt
    test/cockroach/delete.slt
    test/cockroach/discard.slt
    test/cockroach/drop_database.slt
    test/cockroach/drop_table.slt
    test/cockroach/drop_user.slt
    test/cockroach/drop_view.slt
    test/cockroach/errors.slt
    # test/cockroach/exec_hash_join.slt
    # test/cockroach/exec_merge_join.slt
    test/cockroach/exec_window.slt
    # test/cockroach/float.slt
    test/cockroach/inet.slt
    test/cockroach/information_schema.slt
    test/cockroach/insert.slt
    test/cockroach/int_size.slt
    # test/cockroach/join.slt
    test/cockroach/json.slt
    test/cockroach/json_builtins.slt
    # test/cockroach/like.slt
    test/cockroach/limit.slt
    # test/cockroach/lookup_join.slt
    test/cockroach/namespace.slt
    # test/cockroach/no_primary_key.slt
    test/cockroach/order_by.slt
    test/cockroach/ordinal_references.slt
    test/cockroach/ordinality.slt
    test/cockroach/orms-opt.slt
    test/cockroach/orms.slt
    test/cockroach/pg_catalog.slt
    test/cockroach/pgoidtype.slt
    test/cockroach/postgres_jsonb.slt
    # test/cockroach/postgresjoin.slt
    test/cockroach/prepare.slt
    test/cockroach/rename_column.slt
    test/cockroach/rename_constraint.slt
    test/cockroach/rename_database.slt
    test/cockroach/rename_table.slt
    test/cockroach/rename_view.slt
    test/cockroach/returning.slt
    test/cockroach/rows_from.slt
    test/cockroach/scale.slt
    # test/cockroach/select.slt
    # test/cockroach/select_index.slt
    test/cockroach/select_index_flags.slt
    # test/cockroach/select_index_span_ranges.slt
    test/cockroach/select_search_path.slt
    # test/cockroach/select_table_alias.slt
    test/cockroach/shift.slt
    # test/cockroach/sqlsmith.slt
    test/cockroach/srfs.slt
    # test/cockroach/statement_source.slt
    # test/cockroach/suboperators.slt
    # test/cockroach/subquery-opt.slt
    # test/cockroach/subquery.slt
    # test/cockroach/subquery_correlated.slt
    test/cockroach/table.slt
    # test/cockroach/target_names.slt
    # test/cockroach/time.slt
    # test/cockroach/timestamp.slt
    test/cockroach/truncate.slt
    test/cockroach/tuple.slt
    # test/cockroach/typing.slt
    # test/cockroach/union-opt.slt
    # test/cockroach/union.slt
    test/cockroach/update.slt
    test/cockroach/upsert.slt
    test/cockroach/uuid.slt
    # test/cockroach/values.slt
    # test/cockroach/views.slt
    # test/cockroach/where.slt
    test/cockroach/window.slt
    test/cockroach/with.slt
    # test/cockroach/zero.slt
)

export RUST_BACKTRACE=full

SCCACHE_START_SERVER=1 SCCACHE_NO_DAEMON=1 RUST_LOG=debug sccache &

try bin/lint
try cargo fmt --verbose -- --check
try cargo test --verbose
# Intentionally run check last, since otherwise it won't use the cache.
# https://github.com/rust-lang/rust-clippy/issues/3840
try bin/check

# TODO(benesch): maybe some subset of testdrive tests can be run in fast mode?
if [[ ! "$fast" ]]; then
    try cargo build --verbose --release

    try cargo run --verbose --bin sqllogictest --release -- --fail -vv "${sqllogictests[@]}"

    target/release/materialized &
    materialized_pid=$!
    trap "kill -9 $materialized_pid &> /dev/null" EXIT

    # "Wait" for materialized to start up.
    #
    # TODO(benesch): we need proper synchronization here.
    sleep 0.1

    args=()
    [[ "$KAFKA_ADDR" ]] && args+=("--kafka-addr" "$KAFKA_ADDR")
    [[ "$SCHEMA_REGISTRY_URL" ]] && args+=("--schema-registry-url" "$SCHEMA_REGISTRY_URL")
    try target/release/testdrive "${args[@]}" test/*.td
fi

echo "+++ Status report"
echo "$passed/$total commands passed"
if ((passed != total)); then
    exit 1
fi
