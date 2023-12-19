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
# slt-fast.sh — runs fast subset of sqllogictests in CI.

set -euo pipefail

tests=(
    test/sqllogictest/*.slt
    test/sqllogictest/attributes/*.slt \
    test/sqllogictest/explain/*.slt
    test/sqllogictest/autogenerated/*.slt
    test/sqllogictest/transform/*.slt
    test/sqllogictest/cockroach/aggregate.slt
    test/sqllogictest/cockroach/distinct_on.slt
    test/sqllogictest/cockroach/subquery_correlated.slt
)

tests_without_views=(
    test/sqllogictest/alter.slt
    test/sqllogictest/ambiguous_rename.slt
    test/sqllogictest/arithmetic.slt
    test/sqllogictest/array_fill.slt
    test/sqllogictest/arrays.slt
    test/sqllogictest/as_of.slt
    test/sqllogictest/audit_log.slt
    test/sqllogictest/boolean.slt
    test/sqllogictest/bytea.slt
    test/sqllogictest/cast.slt
    test/sqllogictest/char.slt
    test/sqllogictest/chbench.slt
    test/sqllogictest/chr.slt
    test/sqllogictest/cluster.slt
    test/sqllogictest/coercion.slt
    test/sqllogictest/collate.slt
    test/sqllogictest/comparison.slt
    test/sqllogictest/cte.slt
    test/sqllogictest/cte_lowering.slt
    test/sqllogictest/current_database.slt
    test/sqllogictest/cursor.slt
    test/sqllogictest/datediff.slt
    test/sqllogictest/dates-times.slt
    test/sqllogictest/default_privileges.slt
    test/sqllogictest/degenerate.slt
    test/sqllogictest/disambiguate_columns.slt
    test/sqllogictest/distinct_from.slt
    test/sqllogictest/distinct_on.slt
    test/sqllogictest/encode.slt
    test/sqllogictest/errors.slt
    test/sqllogictest/extract.slt
    test/sqllogictest/filter-pushdown.slt
    test/sqllogictest/float.slt
    test/sqllogictest/funcs.slt
    test/sqllogictest/github-11139.slt
    test/sqllogictest/github-11568.slt
    test/sqllogictest/github-13857.slt
    test/sqllogictest/github-14116.slt
    test/sqllogictest/github-15171.slt
    test/sqllogictest/github-16036.slt
    test/sqllogictest/github-17616.slt
    test/sqllogictest/github-17762.slt
    test/sqllogictest/github-17808.slt
    test/sqllogictest/github-18522.slt
    test/sqllogictest/github-18708.slt
    test/sqllogictest/github-19273.slt
    test/sqllogictest/github-7168.slt
    test/sqllogictest/github-7472.slt
    test/sqllogictest/github-7585.slt
    test/sqllogictest/github-8241.slt
    test/sqllogictest/github-8713.slt
    test/sqllogictest/github-8717.slt
    test/sqllogictest/github-9027.slt
    test/sqllogictest/github-9147.slt
    test/sqllogictest/github-9504.slt
    test/sqllogictest/github-9782.slt
    test/sqllogictest/github-9931.slt
    test/sqllogictest/id.slt
    test/sqllogictest/id_reuse.slt
    test/sqllogictest/information_schema_columns.slt
    test/sqllogictest/information_schema_tables.slt
    test/sqllogictest/int2vector.slt
    test/sqllogictest/interval.slt
    test/sqllogictest/joins.slt
    test/sqllogictest/jsonb.slt
    test/sqllogictest/keys.slt
    test/sqllogictest/list.slt
    test/sqllogictest/list_subquery.slt
    test/sqllogictest/managed_cluster.slt
    test/sqllogictest/map.slt
    test/sqllogictest/materialized_views.slt
    test/sqllogictest/mz-resolve-object-name.slt
    test/sqllogictest/mztimestamp.slt
    test/sqllogictest/name_resolution.slt
    test/sqllogictest/not-null-propagation.slt
    test/sqllogictest/numeric.slt
    test/sqllogictest/object_ownership.slt
    test/sqllogictest/oid.slt
    test/sqllogictest/operator.slt
    test/sqllogictest/outer_join.slt
    test/sqllogictest/outer_join_simplification.slt
    test/sqllogictest/parse_ident.slt
    test/sqllogictest/pg_catalog_attribute.slt
    test/sqllogictest/pg_catalog_class.slt
    test/sqllogictest/pg_catalog_matviews.slt
    test/sqllogictest/pg_catalog_namespace.slt
    test/sqllogictest/pg_catalog_proc.slt
    test/sqllogictest/pg_catalog_roles.slt
    test/sqllogictest/pg_catalog_tablespace.slt
    test/sqllogictest/pg_catalog_views.slt
    test/sqllogictest/pg_get_constraintdef.slt
    test/sqllogictest/pg_get_indexdef.slt
    test/sqllogictest/pg_get_viewdef.slt
    test/sqllogictest/pgcli.slt
    test/sqllogictest/pgcrypto.slt
    test/sqllogictest/postgres-incompatibility.slt
    test/sqllogictest/pretty.slt
    test/sqllogictest/privilege_checks.slt
    test/sqllogictest/privilege_grants.slt
    test/sqllogictest/privileges_pg.slt
    test/sqllogictest/quote_ident.slt
    test/sqllogictest/quoting.slt
    test/sqllogictest/range.slt
    test/sqllogictest/rbac_enabled.slt
    test/sqllogictest/record.slt
    test/sqllogictest/recursion_limit.slt
    test/sqllogictest/recursive_type_unioning.slt
    test/sqllogictest/regex.slt
    test/sqllogictest/like.slt
    test/sqllogictest/regressions.slt
    test/sqllogictest/returning.slt
    test/sqllogictest/role.slt
    test/sqllogictest/role_create.slt
    test/sqllogictest/role_membership.slt
    test/sqllogictest/scalar-func-table-position.slt
    test/sqllogictest/scalar_subqueries_select_list.slt
    test/sqllogictest/schemas.slt
    test/sqllogictest/scoping.slt
    test/sqllogictest/secret.slt
    test/sqllogictest/select_all_group_by.slt
    test/sqllogictest/session-window-wmr.slt
    test/sqllogictest/show_create_system_objects.slt
    test/sqllogictest/slt.slt
    test/sqllogictest/source_sizing.slt
    test/sqllogictest/string.slt
    test/sqllogictest/subquery.slt
    test/sqllogictest/subscribe_error.slt
    test/sqllogictest/subscribe_outputs.slt
    test/sqllogictest/subsource.slt
    test/sqllogictest/system-cluster.slt
    test/sqllogictest/table_func.slt
    test/sqllogictest/temporal.slt
    test/sqllogictest/timedomain.slt
    test/sqllogictest/timestamp.slt
    test/sqllogictest/timestamptz.slt
    test/sqllogictest/timezone.slt
    test/sqllogictest/tpch_create_index.slt
    test/sqllogictest/tpch_create_materialized_view.slt
    test/sqllogictest/tpch_select.slt
    test/sqllogictest/transactions.slt
    test/sqllogictest/type-promotion.slt
    test/sqllogictest/typeof.slt
    test/sqllogictest/types.slt
    test/sqllogictest/uniqueness_propagation_filter.slt
    test/sqllogictest/unstable.slt
    test/sqllogictest/updates.slt
    test/sqllogictest/uuid.slt
    test/sqllogictest/vars.slt
    test/sqllogictest/web-console.slt
    test/sqllogictest/window_funcs.slt
    test/sqllogictest/attributes/mir_arity.slt
    test/sqllogictest/attributes/mir_column_types.slt
    test/sqllogictest/attributes/mir_unique_keys.slt
    test/sqllogictest/explain/bad_explain_statements.slt
    test/sqllogictest/explain/decorrelated_plan_as_json.slt
    test/sqllogictest/explain/decorrelated_plan_as_text.slt
    test/sqllogictest/explain/optimized_plan_as_json.slt
    test/sqllogictest/explain/optimized_plan_as_text.slt
    test/sqllogictest/explain/physical_plan_as_json.slt
    test/sqllogictest/explain/physical_plan_as_text.slt
    test/sqllogictest/explain/raw_plan_as_json.slt
    test/sqllogictest/explain/raw_plan_as_text.slt
    test/sqllogictest/autogenerated/all_parts_essential.slt
    test/sqllogictest/autogenerated/char-varchar-comparisons.slt
    test/sqllogictest/transform/aggregation_nullability.slt
    test/sqllogictest/transform/column_knowledge.slt
    test/sqllogictest/transform/dataflow.slt
    test/sqllogictest/transform/demand.slt
    test/sqllogictest/transform/filter_index.slt
    test/sqllogictest/transform/fold_constants.slt
    test/sqllogictest/transform/is_null_propagation.slt
    test/sqllogictest/transform/join_fusion.slt
    test/sqllogictest/transform/join_index.slt
    test/sqllogictest/transform/lifting.slt
    test/sqllogictest/transform/literal_constraints.slt
    test/sqllogictest/transform/literal_lifting.slt
    test/sqllogictest/transform/monotonic.slt
    test/sqllogictest/transform/non_null_requirements.slt
    test/sqllogictest/transform/normalize_lets.slt
    test/sqllogictest/transform/predicate_pushdown.slt
    test/sqllogictest/transform/predicate_reduction.slt
    test/sqllogictest/transform/projection_lifting.slt
    test/sqllogictest/transform/reduce_elision.slt
    test/sqllogictest/transform/reduce_fusion.slt
    test/sqllogictest/transform/reduction_pushdown.slt
    test/sqllogictest/transform/redundant_join.slt
    test/sqllogictest/transform/relation_cse.slt
    test/sqllogictest/transform/scalability.slt
    test/sqllogictest/transform/scalar_cse.slt
    test/sqllogictest/transform/threshold_elision.slt
    test/sqllogictest/transform/topk.slt
    test/sqllogictest/transform/union.slt
    test/sqllogictest/transform/union_cancel.slt
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
    # TODO(benesch): renable a fast subset of the following when performance
    # is restored.
    # test/sqllogictest/sqlite/test/index/between/1/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/commute/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/delete/1/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/in/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/orderby_nosort/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/orderby/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/random/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/index/view/10/slt_good_0.test
    # test/sqllogictest/sqlite/test/random/aggregates/slt_good_0.test
    # test/sqllogictest/sqlite/test/random/expr/slt_good_0.test
    # test/sqllogictest/sqlite/test/random/groupby/slt_good_0.test
    # test/sqllogictest/sqlite/test/random/select/slt_good_0.test
    # test/sqllogictest/sqlite/test/select1.test
    # test/sqllogictest/sqlite/test/select2.test
    # test/sqllogictest/sqlite/test/select3.test
    # test/sqllogictest/sqlite/test/select4.test
    # test/sqllogictest/sqlite/test/select5.test
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

# Exclude tests_without_views from tests
for f in "${tests_without_views[@]}"; do
    tests=("${tests[@]/$f}")
done
# Remove empty entries from tests, since
# sqllogictests emits failures on them.
temp=()
for f in "${tests[@]}"; do
    if [ -n "$f" ]; then
        temp+=("$f")
    fi
done
tests=("${temp[@]}")

sqllogictest -v "${tests[@]}" "$@"
sqllogictest -v "${tests_without_views[@]}" "$@"
