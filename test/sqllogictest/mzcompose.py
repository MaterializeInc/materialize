# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

import argparse
from argparse import Namespace
from pathlib import Path

from materialize import MZ_ROOT, buildkite, ci_util, file_util
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.sql_logic_test import SqlLogicTest

SERVICES = [Cockroach(in_memory=True), SqlLogicTest()]

COCKROACH_DEFAULT_PORT = 26257


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_fast_tests(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run fast SQL logic tests"""
    run_sqllogictest(c, parser, compileFastSltConfig())


def workflow_slow_tests(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run slow SQL logic tests"""
    run_sqllogictest(
        c,
        parser,
        compileSlowSltConfig(),
    )


def workflow_selection(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run specific SQL logic tests using pattern"""
    parser.add_argument(
        "--pattern",
        type=str,
        action="append",
    )

    parser.add_argument(
        "--auto-index-selects",
        default=False,
        type=bool,
        action=argparse.BooleanOptionalAction,
    )

    run_sqllogictest(
        c,
        parser,
        InputBasedSltRunConfig(),
    )


def run_sqllogictest(
    c: Composition, parser: WorkflowArgumentParser, run_config: SltRunConfig
) -> None:
    parser.add_argument("--replicas", default=2, type=int)
    args = parser.parse_args()

    c.up("cockroach")

    junit_report_path = ci_util.junit_report_filename(c.name)
    commands = create_slt_execution_commands(
        args.replicas, run_config, args, junit_report_path
    )

    try:
        for command in commands:
            container_name = "sqllogictest"
            c.run(container_name, *command)
    finally:
        ci_util.upload_junit_report(c.name, MZ_ROOT / junit_report_path)


class SltRunConfig:
    def __init__(self):
        self.steps: list[SltRunStepConfig] = []


class SltRunStepConfig:
    def __init__(self, file_set: set[str], flags: list[str]):
        file_list = list(file_set)
        file_list.sort()
        self.file_list = file_list
        self.flags = flags

    def configure(self, args: Namespace) -> None:
        pass

    def to_command(
        self,
        shard: int,
        shard_count: int,
        replicas: int,
        junit_report_path: Path,
        cockroach_port: int = COCKROACH_DEFAULT_PORT,
    ) -> list[str]:
        sqllogictest_config = [
            f"--junit-report={junit_report_path}",
            f"--postgres-url=postgres://root@cockroach:{cockroach_port}",
            f"--replicas={replicas}",
            f"--shard={shard}",
            f"--shard-count={shard_count}",
        ]
        command = [
            "sqllogictest",
            "-v",
            *self.flags,
            *sqllogictest_config,
            *self.file_list,
        ]

        return command


class InputBasedSltRunStepConfig(SltRunStepConfig):
    def __init__(self) -> None:
        super().__init__(file_set=set(), flags=[])

    def configure(self, args: Namespace) -> None:
        if args.pattern is not None:
            file_list = list(file_util.resolve_paths_with_wildcard(args.pattern))
            file_list.sort()
            self.file_list = file_list

        if args.auto_index_selects:
            self.flags.append("--auto-index-selects")


class InputBasedSltRunConfig(SltRunConfig):
    def __init__(self):
        super().__init__()
        self.steps.append(InputBasedSltRunStepConfig())


class DefaultSltRunStepConfig(SltRunStepConfig):
    def __init__(self, file_set: set[str]):
        super().__init__(
            file_set,
            flags=[],
        )


def create_slt_execution_commands(
    replicas: int, run_config: SltRunConfig, args: Namespace, junit_report_path: Path
) -> list[list[str]]:
    shard = buildkite.get_parallelism_index()
    shard_count = buildkite.get_parallelism_count()

    commands = []

    for step in run_config.steps:
        step.configure(args)
        cmd = step.to_command(shard, shard_count, replicas, junit_report_path)
        commands.append(cmd)

    return commands


def compileFastSltConfig() -> SltRunConfig:
    tests = {
        "test/sqllogictest/*.slt",
        "test/sqllogictest/attributes/*.slt",
        "test/sqllogictest/explain/*.slt",
        "test/sqllogictest/autogenerated/*.slt",
        "test/sqllogictest/transform/*.slt",
        "test/sqllogictest/release-builds/*.slt",
        "test/sqllogictest/cockroach/aggregate.slt",
        "test/sqllogictest/cockroach/distinct_on.slt",
        "test/sqllogictest/cockroach/subquery_correlated.slt",
    }

    tests_without_views = {
        "test/sqllogictest/alter.slt",
        "test/sqllogictest/ambiguous_rename.slt",
        "test/sqllogictest/arithmetic.slt",
        "test/sqllogictest/array_fill.slt",
        "test/sqllogictest/arrays.slt",
        "test/sqllogictest/as_of.slt",
        "test/sqllogictest/audit_log.slt",
        "test/sqllogictest/boolean.slt",
        "test/sqllogictest/bytea.slt",
        "test/sqllogictest/cast.slt",
        "test/sqllogictest/char.slt",
        "test/sqllogictest/chbench.slt",
        "test/sqllogictest/chr.slt",
        "test/sqllogictest/cluster.slt",
        "test/sqllogictest/coercion.slt",
        "test/sqllogictest/collate.slt",
        "test/sqllogictest/comparison.slt",
        "test/sqllogictest/cte.slt",
        "test/sqllogictest/cte_lowering.slt",
        "test/sqllogictest/current_database.slt",
        "test/sqllogictest/cursor.slt",
        "test/sqllogictest/datediff.slt",
        "test/sqllogictest/dates-times.slt",
        "test/sqllogictest/default_privileges.slt",
        "test/sqllogictest/degenerate.slt",
        "test/sqllogictest/disambiguate_columns.slt",
        "test/sqllogictest/distinct_from.slt",
        "test/sqllogictest/distinct_on.slt",
        "test/sqllogictest/encode.slt",
        "test/sqllogictest/errors.slt",
        "test/sqllogictest/extract.slt",
        "test/sqllogictest/filter-pushdown.slt",
        "test/sqllogictest/float.slt",
        "test/sqllogictest/funcs.slt",
        "test/sqllogictest/github-11139.slt",
        "test/sqllogictest/github-11568.slt",
        "test/sqllogictest/github-13857.slt",
        "test/sqllogictest/github-14116.slt",
        "test/sqllogictest/github-15171.slt",
        "test/sqllogictest/github-16036.slt",
        "test/sqllogictest/github-17616.slt",
        "test/sqllogictest/github-17762.slt",
        "test/sqllogictest/github-17808.slt",
        "test/sqllogictest/github-18522.slt",
        "test/sqllogictest/github-18708.slt",
        "test/sqllogictest/github-19273.slt",
        "test/sqllogictest/github-7168.slt",
        "test/sqllogictest/github-7472.slt",
        "test/sqllogictest/github-7585.slt",
        "test/sqllogictest/github-8241.slt",
        "test/sqllogictest/github-8713.slt",
        "test/sqllogictest/github-8717.slt",
        "test/sqllogictest/github-9027.slt",
        "test/sqllogictest/github-9147.slt",
        "test/sqllogictest/github-9504.slt",
        "test/sqllogictest/github-9782.slt",
        "test/sqllogictest/github-9931.slt",
        "test/sqllogictest/id.slt",
        "test/sqllogictest/id_reuse.slt",
        "test/sqllogictest/information_schema_columns.slt",
        "test/sqllogictest/information_schema_tables.slt",
        "test/sqllogictest/int2vector.slt",
        "test/sqllogictest/interval.slt",
        "test/sqllogictest/joins.slt",
        "test/sqllogictest/jsonb.slt",
        "test/sqllogictest/keys.slt",
        "test/sqllogictest/like.slt",
        "test/sqllogictest/list.slt",
        "test/sqllogictest/list_subquery.slt",
        "test/sqllogictest/managed_cluster.slt",
        "test/sqllogictest/map.slt",
        "test/sqllogictest/materialized_views.slt",
        "test/sqllogictest/mz_introspection_index_accounting.slt",
        "test/sqllogictest/mztimestamp.slt",
        "test/sqllogictest/name_resolution.slt",
        "test/sqllogictest/not-null-propagation.slt",
        "test/sqllogictest/numeric.slt",
        "test/sqllogictest/object_ownership.slt",
        "test/sqllogictest/oid.slt",
        "test/sqllogictest/operator.slt",
        "test/sqllogictest/outer_join.slt",
        "test/sqllogictest/outer_join_simplification.slt",
        "test/sqllogictest/parse_ident.slt",
        "test/sqllogictest/pg_catalog_attribute.slt",
        "test/sqllogictest/pg_catalog_class.slt",
        "test/sqllogictest/pg_catalog_matviews.slt",
        "test/sqllogictest/pg_catalog_namespace.slt",
        "test/sqllogictest/pg_catalog_proc.slt",
        "test/sqllogictest/pg_catalog_roles.slt",
        "test/sqllogictest/pg_catalog_tablespace.slt",
        "test/sqllogictest/pg_catalog_views.slt",
        "test/sqllogictest/pg_get_constraintdef.slt",
        "test/sqllogictest/pg_get_indexdef.slt",
        "test/sqllogictest/pg_get_viewdef.slt",
        "test/sqllogictest/pgcli.slt",
        "test/sqllogictest/pgcrypto.slt",
        "test/sqllogictest/postgres-incompatibility.slt",
        "test/sqllogictest/pretty.slt",
        "test/sqllogictest/privilege_checks.slt",
        "test/sqllogictest/privilege_grants.slt",
        "test/sqllogictest/privileges_pg.slt",
        "test/sqllogictest/quote_ident.slt",
        "test/sqllogictest/quoting.slt",
        "test/sqllogictest/range.slt",
        "test/sqllogictest/rbac_enabled.slt",
        "test/sqllogictest/record.slt",
        "test/sqllogictest/recursion_limit.slt",
        "test/sqllogictest/recursive_type_unioning.slt",
        "test/sqllogictest/regclass.slt",
        "test/sqllogictest/regex.slt",
        "test/sqllogictest/regproc.slt",
        "test/sqllogictest/regressions.slt",
        "test/sqllogictest/regtype.slt",
        "test/sqllogictest/returning.slt",
        "test/sqllogictest/role.slt",
        "test/sqllogictest/role_create.slt",
        "test/sqllogictest/role_membership.slt",
        "test/sqllogictest/scalar-func-table-position.slt",
        "test/sqllogictest/scalar_subqueries_select_list.slt",
        "test/sqllogictest/schemas.slt",
        "test/sqllogictest/scoping.slt",
        "test/sqllogictest/secret.slt",
        "test/sqllogictest/select_all_group_by.slt",
        "test/sqllogictest/session-window-wmr.slt",
        "test/sqllogictest/show_create_system_objects.slt",
        "test/sqllogictest/slt.slt",
        "test/sqllogictest/source_sizing.slt",
        "test/sqllogictest/string.slt",
        "test/sqllogictest/subquery.slt",
        "test/sqllogictest/subscribe_error.slt",
        "test/sqllogictest/subscribe_outputs.slt",
        "test/sqllogictest/subsource.slt",
        "test/sqllogictest/system-cluster.slt",
        "test/sqllogictest/table_func.slt",
        "test/sqllogictest/temporal.slt",
        "test/sqllogictest/timedomain.slt",
        "test/sqllogictest/timestamp.slt",
        "test/sqllogictest/timestamptz.slt",
        "test/sqllogictest/timezone.slt",
        "test/sqllogictest/tpch_create_index.slt",
        "test/sqllogictest/tpch_create_materialized_view.slt",
        "test/sqllogictest/tpch_select.slt",
        "test/sqllogictest/transactions.slt",
        "test/sqllogictest/type-promotion.slt",
        "test/sqllogictest/typeof.slt",
        "test/sqllogictest/types.slt",
        "test/sqllogictest/uniqueness_propagation_filter.slt",
        "test/sqllogictest/unstable.slt",
        "test/sqllogictest/updates.slt",
        "test/sqllogictest/uuid.slt",
        "test/sqllogictest/vars.slt",
        "test/sqllogictest/web-console.slt",
        "test/sqllogictest/window_funcs.slt",
        "test/sqllogictest/attributes/mir_arity.slt",
        "test/sqllogictest/attributes/mir_column_types.slt",
        "test/sqllogictest/attributes/mir_unique_keys.slt",
        "test/sqllogictest/explain/bad_explain_statements.slt",
        "test/sqllogictest/explain/decorrelated_plan_as_json.slt",
        "test/sqllogictest/explain/decorrelated_plan_as_text.slt",
        "test/sqllogictest/explain/optimized_plan_as_json.slt",
        "test/sqllogictest/explain/optimized_plan_as_text.slt",
        "test/sqllogictest/explain/physical_plan_as_json.slt",
        "test/sqllogictest/explain/physical_plan_as_text.slt",
        "test/sqllogictest/explain/raw_plan_as_json.slt",
        "test/sqllogictest/explain/raw_plan_as_text.slt",
        "test/sqllogictest/autogenerated/all_parts_essential.slt",
        "test/sqllogictest/autogenerated/char-varchar-comparisons.slt",
        "test/sqllogictest/transform/aggregation_nullability.slt",
        "test/sqllogictest/transform/column_knowledge.slt",
        "test/sqllogictest/transform/dataflow.slt",
        "test/sqllogictest/transform/demand.slt",
        "test/sqllogictest/transform/filter_index.slt",
        "test/sqllogictest/transform/fold_constants.slt",
        "test/sqllogictest/transform/is_null_propagation.slt",
        "test/sqllogictest/transform/join_fusion.slt",
        "test/sqllogictest/transform/join_index.slt",
        "test/sqllogictest/transform/lifting.slt",
        "test/sqllogictest/transform/literal_constraints.slt",
        "test/sqllogictest/transform/literal_lifting.slt",
        "test/sqllogictest/transform/monotonic.slt",
        "test/sqllogictest/transform/non_null_requirements.slt",
        "test/sqllogictest/transform/normalize_lets.slt",
        "test/sqllogictest/transform/predicate_pushdown.slt",
        "test/sqllogictest/transform/predicate_reduction.slt",
        "test/sqllogictest/transform/projection_lifting.slt",
        "test/sqllogictest/transform/reduce_elision.slt",
        "test/sqllogictest/transform/reduce_fusion.slt",
        "test/sqllogictest/transform/reduction_pushdown.slt",
        "test/sqllogictest/transform/redundant_join.slt",
        "test/sqllogictest/transform/relation_cse.slt",
        "test/sqllogictest/transform/scalability.slt",
        "test/sqllogictest/transform/scalar_cse.slt",
        "test/sqllogictest/transform/threshold_elision.slt",
        "test/sqllogictest/transform/topk.slt",
        "test/sqllogictest/transform/union.slt",
        "test/sqllogictest/transform/union_cancel.slt",
        # "test/sqllogictest/sqlite/test/evidence/in1.test",
        "test/sqllogictest/sqlite/test/evidence/in2.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_aggfunc.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_createtrigger.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_createview.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_dropindex.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_droptable.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_droptrigger.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_dropview.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_reindex.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_replace.test",
        "test/sqllogictest/sqlite/test/evidence/slt_lang_update.test",
        # TODO(benesch): renable a fast subset of the following when performance is restored.
        # "test/sqllogictest/sqlite/test/index/between/1/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/commute/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/delete/1/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/in/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/orderby_nosort/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/orderby/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/random/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/index/view/10/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/random/aggregates/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/random/expr/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/random/groupby/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/random/select/slt_good_0.test",
        # "test/sqllogictest/sqlite/test/select1.test",
        # "test/sqllogictest/sqlite/test/select2.test",
        # "test/sqllogictest/sqlite/test/select3.test",
        # "test/sqllogictest/sqlite/test/select4.test",
        # "test/sqllogictest/sqlite/test/select5.test",
        "test/sqllogictest/cockroach/alias_types.slt",
        "test/sqllogictest/cockroach/alter_column_type.slt",
        "test/sqllogictest/cockroach/alter_table.slt",
        "test/sqllogictest/cockroach/apply_join.slt",
        "test/sqllogictest/cockroach/array.slt",
        "test/sqllogictest/cockroach/as_of.slt",
        "test/sqllogictest/cockroach/bit.slt",
        # "test/sqllogictest/cockroach/builtin_function.slt",
        "test/sqllogictest/cockroach/bytes.slt",
        "test/sqllogictest/cockroach/case_sensitive_names.slt",
        # "test/sqllogictest/cockroach/collatedstring_constraint.slt",
        "test/sqllogictest/cockroach/collatedstring_index1.slt",
        "test/sqllogictest/cockroach/collatedstring_index2.slt",
        "test/sqllogictest/cockroach/collatedstring_normalization.slt",
        "test/sqllogictest/cockroach/collatedstring_nullinindex.slt",
        "test/sqllogictest/cockroach/collatedstring_uniqueindex1.slt",
        "test/sqllogictest/cockroach/collatedstring_uniqueindex2.slt",
        "test/sqllogictest/cockroach/collatedstring.slt",
        "test/sqllogictest/cockroach/computed.slt",
        # "test/sqllogictest/cockroach/conditional.slt",
        "test/sqllogictest/cockroach/create_as.slt",
        "test/sqllogictest/cockroach/custom_escape_character.slt",
        "test/sqllogictest/cockroach/database.slt",
        # "test/sqllogictest/cockroach/datetime.slt",
        # "test/sqllogictest/cockroach/decimal.slt",
        "test/sqllogictest/cockroach/delete.slt",
        "test/sqllogictest/cockroach/discard.slt",
        "test/sqllogictest/cockroach/drop_database.slt",
        "test/sqllogictest/cockroach/drop_table.slt",
        "test/sqllogictest/cockroach/drop_user.slt",
        "test/sqllogictest/cockroach/drop_view.slt",
        "test/sqllogictest/cockroach/errors.slt",
        # "test/sqllogictest/cockroach/exec_hash_join.slt",
        # "test/sqllogictest/cockroach/exec_merge_join.slt",
        "test/sqllogictest/cockroach/exec_window.slt",
        "test/sqllogictest/cockroach/extract.slt",
        # "test/sqllogictest/cockroach/float.slt",
        "test/sqllogictest/cockroach/inet.slt",
        "test/sqllogictest/cockroach/information_schema.slt",
        "test/sqllogictest/cockroach/insert.slt",
        "test/sqllogictest/cockroach/int_size.slt",
        # "test/sqllogictest/cockroach/join.slt",
        # "test/sqllogictest/cockroach/json_builtins.slt",
        # "test/sqllogictest/cockroach/json.slt",
        "test/sqllogictest/cockroach/like.slt",
        "test/sqllogictest/cockroach/limit.slt",
        # "test/sqllogictest/cockroach/lookup_join.slt",
        "test/sqllogictest/cockroach/namespace.slt",
        # "test/sqllogictest/cockroach/no_primary_key.slt",
        "test/sqllogictest/cockroach/order_by.slt",
        "test/sqllogictest/cockroach/ordinal_references.slt",
        "test/sqllogictest/cockroach/ordinality.slt",
        "test/sqllogictest/cockroach/orms-opt.slt",
        "test/sqllogictest/cockroach/orms.slt",
        "test/sqllogictest/cockroach/pg_catalog.slt",
        "test/sqllogictest/cockroach/pgoidtype.slt",
        # "test/sqllogictest/cockroach/postgres_jsonb.slt",
        # "test/sqllogictest/cockroach/postgresjoin.slt",
        "test/sqllogictest/cockroach/prepare.slt",
        "test/sqllogictest/cockroach/rename_column.slt",
        "test/sqllogictest/cockroach/rename_constraint.slt",
        "test/sqllogictest/cockroach/rename_database.slt",
        "test/sqllogictest/cockroach/rename_table.slt",
        "test/sqllogictest/cockroach/rename_view.slt",
        "test/sqllogictest/cockroach/returning.slt",
        "test/sqllogictest/cockroach/rows_from.slt",
        "test/sqllogictest/cockroach/scale.slt",
        "test/sqllogictest/cockroach/select_index_flags.slt",
        # "test/sqllogictest/cockroach/select_index_span_ranges.slt",
        # "test/sqllogictest/cockroach/select_index.slt",
        "test/sqllogictest/cockroach/select_search_path.slt",
        "test/sqllogictest/cockroach/select_table_alias.slt",
        # "test/sqllogictest/cockroach/select.slt",
        "test/sqllogictest/cockroach/shift.slt",
        # "test/sqllogictest/cockroach/sqlsmith.slt",
        "test/sqllogictest/cockroach/srfs.slt",
        # "test/sqllogictest/cockroach/statement_source.slt",
        # "test/sqllogictest/cockroach/suboperators.slt",
        # "test/sqllogictest/cockroach/subquery-opt.slt",
        # "test/sqllogictest/cockroach/subquery.slt",
        "test/sqllogictest/cockroach/table.slt",
        # "test/sqllogictest/cockroach/target_names.slt",
        # "test/sqllogictest/cockroach/time.slt",
        "test/sqllogictest/cockroach/timestamp.slt",
        "test/sqllogictest/cockroach/truncate.slt",
        "test/sqllogictest/cockroach/tuple.slt",
        # "test/sqllogictest/cockroach/typing.slt",
        # "test/sqllogictest/cockroach/union-opt.slt",
        "test/sqllogictest/cockroach/union.slt",
        "test/sqllogictest/cockroach/update.slt",
        "test/sqllogictest/cockroach/upsert.slt",
        "test/sqllogictest/cockroach/uuid.slt",
        "test/sqllogictest/cockroach/values.slt",
        # "test/sqllogictest/cockroach/views.slt",
        # "test/sqllogictest/cockroach/where.slt",
        "test/sqllogictest/cockroach/window.slt",
        "test/sqllogictest/cockroach/with.slt",
        # "test/sqllogictest/cockroach/zero.slt",
        "test/sqllogictest/postgres/float4.slt",
        "test/sqllogictest/postgres/float8.slt",
        "test/sqllogictest/postgres/join-lateral.slt",
        "test/sqllogictest/postgres/regex.slt",
        "test/sqllogictest/postgres/subselect.slt",
        "test/sqllogictest/postgres/pgcrypto/*.slt",
    }

    tests = file_util.resolve_paths_with_wildcard(tests)
    tests_without_views = file_util.resolve_paths_with_wildcard(tests_without_views)
    tests_with_views = tests - tests_without_views

    config = SltRunConfig()
    config.steps.append(DefaultSltRunStepConfig(tests_without_views))
    config.steps.append(SltRunStepConfig(tests_with_views, ["--auto-index-selects"]))
    return config


def compileSlowSltConfig() -> SltRunConfig:
    config = SltRunConfig()

    # All CockroachDB and PostgreSQL SLTs can be run with --auto-index-selects, but require --no-fail
    cockroach_and_pg_tests = {
        "test/sqllogictest/cockroach/*.slt",
        "test/sqllogictest/postgres/*.slt",
        "test/sqllogictest/postgres/pgcrypto/*.slt",
    }
    cockroach_and_pg_tests = file_util.resolve_paths_with_wildcard(
        cockroach_and_pg_tests
    )

    config.steps.append(
        SltRunStepConfig(cockroach_and_pg_tests, ["--auto-index-selects", "--no-fail"])
    )

    tests = {
        "test/sqllogictest/*.slt",
        "test/sqllogictest/attributes/*.slt",
        "test/sqllogictest/introspection/*.slt",
        "test/sqllogictest/explain/*.slt",
        "test/sqllogictest/transform/*.slt",
        "test/sqllogictest/transform/fold_vs_dataflow/*.slt",
        "test/sqllogictest/transform/notice/*.slt",
        "test/sqllogictest/special/*",
    }
    tests_without_views_and_replica = {
        # errors:
        # https://github.com/MaterializeInc/materialize/issues/20534
        "test/sqllogictest/list.slt",
        # transactions:
        "test/sqllogictest/github-11568.slt",
        "test/sqllogictest/introspection/cluster_log_compaction.slt",
        "test/sqllogictest/timedomain.slt",
        "test/sqllogictest/transactions.slt",
        # depends on unmaterializable functions
        "test/sqllogictest/regclass.slt",
        "test/sqllogictest/regproc.slt",
        "test/sqllogictest/regtype.slt",
        # different outputs:
        # seems expected for audit log to be different
        "test/sqllogictest/audit_log.slt",
        # different indexes auto-created
        "test/sqllogictest/cluster.slt",
        # different indexes auto-created
        "test/sqllogictest/object_ownership.slt",
        # https://github.com/MaterializeInc/materialize/issues/20110
        "test/sqllogictest/interval.slt",
        # https://github.com/MaterializeInc/materialize/issues/20110
        "test/sqllogictest/operator.slt",
        # specific replica size tested:
        "test/sqllogictest/managed_cluster.slt",
        "test/sqllogictest/web-console.slt",
        "test/sqllogictest/show_clusters.slt",
    }

    tests = file_util.resolve_paths_with_wildcard(tests)
    tests_without_views_and_replica = file_util.resolve_paths_with_wildcard(
        tests_without_views_and_replica
    )
    tests_with_views_or_replica = tests - tests_without_views_and_replica

    config.steps.append(
        SltRunStepConfig(tests_with_views_or_replica, ["--auto-index-selects"])
    )
    config.steps.append(SltRunStepConfig(tests_without_views_and_replica, []))

    # Due to performance issues (see below), we pick two selected SLTs from
    # the SQLite corpus that we can reasonably run with --auto-index-selects
    # and that include min/max query patterns. Note that none of the SLTs in
    # the corpus we presently use from SQLite contain top-k patterns.
    tests_with_views = {
        "test/sqllogictest/sqlite/test/index/random/1000/slt_good_0.test",
        "test/sqllogictest/sqlite/test/random/aggregates/slt_good_129.test",
    }

    tests = set(file_util.get_recursive_file_list("test/sqllogictest/sqlite/test"))
    tests_without_views = tests - tests_with_views

    # Run selected tests with --auto-index-selects
    config.steps.append(
        SltRunStepConfig(
            tests_with_views,
            ["--auto-index-selects", "--enable-table-keys"],
        )
    )

    # Too slow to run with --auto-index-selects, can't run together with
    # --auto-transactions, no differences seen in previous run. We might want to
    # revisit and see if we can periodically test them, even if it takes 2-3 days
    # for the run to finish.
    config.steps.append(
        SltRunStepConfig(
            tests_without_views,
            ["--auto-transactions", "--auto-index-tables", "--enable-table-keys"],
        )
    )

    return config
