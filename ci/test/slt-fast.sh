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
# slt-fast.sh — runs fast subset of sqllogictests in CI.

set -euo pipefail

. misc/shlib/shlib.bash

if [[ ! "${BUILDKITE-}" ]]; then
    sqllogictest() {
        cargo run --release --bin sqllogictest -- "$@"
    }
fi

export RUST_BACKTRACE=full

tests=(
    test/sqllogictest/*.slt
    test/sqllogictest/transform/*.slt
    # test/sqllogictest/sqlite/test/evidence/in1.test
    test/sqllogictest/sqlite/test/evidence/in2.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_aggfunc.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_createtrigger.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_createview.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_dropindex.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_droptable.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_droptrigger.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_dropview.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_reindex.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_replace.test
    test/sqllogictest/sqlite/test/evidence/slt_lang_update.test
    # test/sqllogictest/sqlite/test/index/between/1/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/commute/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/delete/1/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/in/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/orderby_nosort/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/orderby/10/slt_good_0.test
    test/sqllogictest/sqlite/test/index/random/10/slt_good_0.test
    test/sqllogictest/sqlite/test/index/view/10/slt_good_0.test
    test/sqllogictest/sqlite/test/random/aggregates/slt_good_0.test
    test/sqllogictest/sqlite/test/random/expr/slt_good_0.test
    test/sqllogictest/sqlite/test/random/groupby/slt_good_0.test
    test/sqllogictest/sqlite/test/random/select/slt_good_0.test
    # test/sqllogictest/sqlite/test/select1.test
    # test/sqllogictest/sqlite/test/select2.test
    # test/sqllogictest/sqlite/test/select3.test
    # test/sqllogictest/sqlite/test/select4.test
    test/sqllogictest/sqlite/test/select5.test
    test/sqllogictest/cockroach/aggregate.slt
    test/sqllogictest/cockroach/alias_types.slt
    test/sqllogictest/cockroach/alter_column_type.slt
    test/sqllogictest/cockroach/alter_table.slt
    test/sqllogictest/cockroach/apply_join.slt
    test/sqllogictest/cockroach/array.slt
    test/sqllogictest/cockroach/as_of.slt
    test/sqllogictest/cockroach/bit.slt
    # test/sqllogictest/cockroach/builtin_function.slt
    test/sqllogictest/cockroach/bytes.slt
    test/sqllogictest/cockroach/case_sensitive_names.slt
    # test/sqllogictest/cockroach/collatedstring_constraint.slt
    test/sqllogictest/cockroach/collatedstring_index1.slt
    test/sqllogictest/cockroach/collatedstring_index2.slt
    test/sqllogictest/cockroach/collatedstring_normalization.slt
    test/sqllogictest/cockroach/collatedstring_nullinindex.slt
    test/sqllogictest/cockroach/collatedstring_uniqueindex1.slt
    test/sqllogictest/cockroach/collatedstring_uniqueindex2.slt
    test/sqllogictest/cockroach/collatedstring.slt
    test/sqllogictest/cockroach/computed.slt
    # test/sqllogictest/cockroach/conditional.slt
    test/sqllogictest/cockroach/create_as.slt
    test/sqllogictest/cockroach/custom_escape_character.slt
    test/sqllogictest/cockroach/database.slt
    # test/sqllogictest/cockroach/datetime.slt
    # test/sqllogictest/cockroach/decimal.slt
    test/sqllogictest/cockroach/delete.slt
    test/sqllogictest/cockroach/discard.slt
    test/sqllogictest/cockroach/distinct_on.slt
    test/sqllogictest/cockroach/drop_database.slt
    test/sqllogictest/cockroach/drop_table.slt
    test/sqllogictest/cockroach/drop_user.slt
    test/sqllogictest/cockroach/drop_view.slt
    test/sqllogictest/cockroach/errors.slt
    # test/sqllogictest/cockroach/exec_hash_join.slt
    # test/sqllogictest/cockroach/exec_merge_join.slt
    test/sqllogictest/cockroach/exec_window.slt
    test/sqllogictest/cockroach/extract.slt
    # test/sqllogictest/cockroach/float.slt
    test/sqllogictest/cockroach/inet.slt
    test/sqllogictest/cockroach/information_schema.slt
    test/sqllogictest/cockroach/insert.slt
    test/sqllogictest/cockroach/int_size.slt
    # test/sqllogictest/cockroach/join.slt
    # test/sqllogictest/cockroach/json_builtins.slt
    # test/sqllogictest/cockroach/json.slt
    test/sqllogictest/cockroach/like.slt
    test/sqllogictest/cockroach/limit.slt
    # test/sqllogictest/cockroach/lookup_join.slt
    test/sqllogictest/cockroach/namespace.slt
    # test/sqllogictest/cockroach/no_primary_key.slt
    test/sqllogictest/cockroach/order_by.slt
    test/sqllogictest/cockroach/ordinal_references.slt
    test/sqllogictest/cockroach/ordinality.slt
    test/sqllogictest/cockroach/orms-opt.slt
    test/sqllogictest/cockroach/orms.slt
    test/sqllogictest/cockroach/pg_catalog.slt
    test/sqllogictest/cockroach/pgoidtype.slt
    # test/sqllogictest/cockroach/postgres_jsonb.slt
    # test/sqllogictest/cockroach/postgresjoin.slt
    test/sqllogictest/cockroach/prepare.slt
    test/sqllogictest/cockroach/rename_column.slt
    test/sqllogictest/cockroach/rename_constraint.slt
    test/sqllogictest/cockroach/rename_database.slt
    test/sqllogictest/cockroach/rename_table.slt
    test/sqllogictest/cockroach/rename_view.slt
    test/sqllogictest/cockroach/returning.slt
    test/sqllogictest/cockroach/rows_from.slt
    test/sqllogictest/cockroach/scale.slt
    test/sqllogictest/cockroach/select_index_flags.slt
    # test/sqllogictest/cockroach/select_index_span_ranges.slt
    # test/sqllogictest/cockroach/select_index.slt
    test/sqllogictest/cockroach/select_search_path.slt
    test/sqllogictest/cockroach/select_table_alias.slt
    # test/sqllogictest/cockroach/select.slt
    test/sqllogictest/cockroach/shift.slt
    # test/sqllogictest/cockroach/sqlsmith.slt
    test/sqllogictest/cockroach/srfs.slt
    # test/sqllogictest/cockroach/statement_source.slt
    # test/sqllogictest/cockroach/suboperators.slt
    test/sqllogictest/cockroach/subquery_correlated.slt
    # test/sqllogictest/cockroach/subquery-opt.slt
    # test/sqllogictest/cockroach/subquery.slt
    test/sqllogictest/cockroach/table.slt
    # test/sqllogictest/cockroach/target_names.slt
    # test/sqllogictest/cockroach/time.slt
    test/sqllogictest/cockroach/timestamp.slt
    test/sqllogictest/cockroach/truncate.slt
    test/sqllogictest/cockroach/tuple.slt
    # test/sqllogictest/cockroach/typing.slt
    # test/sqllogictest/cockroach/union-opt.slt
    test/sqllogictest/cockroach/union.slt
    test/sqllogictest/cockroach/update.slt
    test/sqllogictest/cockroach/upsert.slt
    test/sqllogictest/cockroach/uuid.slt
    test/sqllogictest/cockroach/values.slt
    # test/sqllogictest/cockroach/views.slt
    # test/sqllogictest/cockroach/where.slt
    test/sqllogictest/cockroach/window.slt
    test/sqllogictest/cockroach/with.slt
    # test/sqllogictest/cockroach/zero.slt
    test/sqllogictest/postgres/float4.slt
    test/sqllogictest/postgres/float8.slt
    test/sqllogictest/postgres/join-lateral.slt
    test/sqllogictest/postgres/regex.slt
    test/sqllogictest/postgres/subselect.slt
    test/sqllogictest/postgres/pgcrypto/*.slt
)

if [[ "${BUILDKITE-}" ]]; then
    await_postgres -h postgres -p 5432
fi

sqllogictest -v "${tests[@]}"
