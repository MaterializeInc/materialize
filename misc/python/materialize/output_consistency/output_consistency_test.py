# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

import psycopg
from psycopg import Connection
from psycopg.errors import OperationalError

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
)
from materialize.output_consistency.execution.query_output_mode import QueryOutputMode
from materialize.output_consistency.execution.sql_executor import (
    DryRunSqlExecutor,
    MzDatabaseSqlExecutor,
    PgWireDatabaseSqlExecutor,
    SqlExecutor,
)
from materialize.output_consistency.execution.sql_executors import SqlExecutors
from materialize.output_consistency.generators.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.generators.query_generator import QueryGenerator
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
)
from materialize.output_consistency.ignore_filter.internal_output_inconsistency_ignore_filter import (
    InternalOutputInconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.scenarios.evaluation_scenario import (
    EvaluationScenario,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.runner.test_runner import ConsistencyTestRunner
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker
from materialize.output_consistency.status.test_summary import ConsistencyTestSummary
from materialize.output_consistency.validation.error_message_normalizer import (
    ErrorMessageNormalizer,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb


class OutputConsistencyTest:
    def run_output_consistency_tests(
        self,
        default_connection: Connection,
        mz_system_connection: Connection,
        args: argparse.Namespace,
        query_output_mode: QueryOutputMode,
        override_max_runtime_in_sec: int | None = None,
    ) -> ConsistencyTestSummary:
        """Entry point for output consistency tests"""

        return self._run_output_consistency_tests_internal(
            default_connection,
            mz_system_connection,
            args.seed,
            args.dry_run,
            args.fail_fast,
            args.verbose,
            args.max_cols_per_query,
            override_max_runtime_in_sec or args.max_runtime_in_sec,
            args.max_iterations,
            args.max_failures_until_abort,
            args.avoid_expressions_expecting_db_error,
            args.disable_predefined_queries,
            query_output_mode=query_output_mode,
        )

    def parse_output_consistency_input_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> argparse.Namespace:
        parser.add_argument("--seed", default="0", type=str)
        parser.add_argument(
            "--dry-run", default=False, type=bool, action=argparse.BooleanOptionalAction
        )
        parser.add_argument(
            "--fail-fast",
            default=False,
            type=bool,
            action=argparse.BooleanOptionalAction,
        )
        parser.add_argument(
            "--verbose",
            default=False,
            type=bool,
            action=argparse.BooleanOptionalAction,
        )
        parser.add_argument("--max-cols-per-query", default=20, type=int)
        parser.add_argument("--max-runtime-in-sec", default=600, type=int)
        parser.add_argument("--max-iterations", default=100000, type=int)
        parser.add_argument("--max-failures-until-abort", default=15, type=int)
        parser.add_argument(
            "--avoid-expressions-expecting-db-error",
            default=False,
            type=bool,
            action=argparse.BooleanOptionalAction,
        )
        parser.add_argument(
            "--disable-predefined-queries",
            default=False,
            type=bool,
            action=argparse.BooleanOptionalAction,
        )

        return parser.parse_args()

    def _run_output_consistency_tests_internal(
        self,
        default_connection: Connection,
        mz_system_connection: Connection,
        random_seed: str,
        dry_run: bool,
        fail_fast: bool,
        verbose_output: bool,
        max_cols_per_query: int,
        max_runtime_in_sec: int,
        max_iterations: int,
        max_failures_until_abort: int,
        avoid_expressions_expecting_db_error: bool,
        disable_predefined_queries: bool,
        query_output_mode: QueryOutputMode,
    ) -> ConsistencyTestSummary:
        input_data = self.create_input_data()

        scenario = self.get_scenario()

        if fail_fast:
            max_failures_until_abort = 1

        config = ConsistencyTestConfiguration(
            random_seed=random_seed,
            scenario=scenario,
            dry_run=dry_run,
            verbose_output=verbose_output,
            max_cols_per_query=max_cols_per_query,
            max_runtime_in_sec=max_runtime_in_sec,
            max_iterations=max_iterations,
            max_failures_until_abort=max_failures_until_abort,
            avoid_expressions_expecting_db_error=avoid_expressions_expecting_db_error,
            queries_per_tx=20,
            max_pending_expressions=100,
            use_autocommit=True,
            split_and_retry_on_db_error=True,
            print_reproduction_code=True,
            disable_predefined_queries=disable_predefined_queries,
            query_output_mode=query_output_mode,
            vertical_join_tables=4,
        )

        output_printer = OutputPrinter(input_data, config.query_output_mode)
        output_printer.print_config(config)
        config.validate()

        randomized_picker = RandomizedPicker(config)

        sql_executors = self.create_sql_executors(
            config, default_connection, mz_system_connection, output_printer
        )

        evaluation_strategies = self.create_evaluation_strategies(sql_executors)

        # prerequisite: sql_executors need to be created
        self.filter_input_data(input_data)

        # prerequisite: sql_executors need to be created
        ignore_filter = self.create_inconsistency_ignore_filter()

        # prerequisite: input data needs to be filtered
        expression_generator = ExpressionGenerator(
            config, randomized_picker, input_data
        )
        query_generator = QueryGenerator(
            config, randomized_picker, input_data, expression_generator, ignore_filter
        )
        output_comparator = self.create_result_comparator(ignore_filter)

        output_printer.print_info(sql_executors.get_database_infos())
        output_printer.print_empty_line()

        # prerequisite: input data needs to be filtered
        output_printer.print_info(input_data.get_stats())
        output_printer.print_empty_line()

        if not self.shall_run(sql_executors):
            output_printer.print_info("Not running the test, criteria are not met.")
            return ConsistencyTestSummary()

        test_runner = ConsistencyTestRunner(
            config,
            input_data,
            evaluation_strategies,
            expression_generator,
            query_generator,
            output_comparator,
            sql_executors,
            randomized_picker,
            ignore_filter,
            output_printer,
        )
        test_runner.setup()

        output_printer.start_section("Test remarks")

        if not config.verbose_output:
            output_printer.print_info(
                "Printing only queries with inconsistencies or warnings in non-verbose mode."
            )
            output_printer.print_empty_line()

        test_summary = test_runner.start()

        output_printer.print_test_summary(test_summary)

        return test_summary

    def shall_run(self, sql_executors: SqlExecutors) -> bool:
        return True

    def create_input_data(self) -> ConsistencyTestInputData:
        return ConsistencyTestInputData()

    def filter_input_data(self, input_data: ConsistencyTestInputData) -> None:
        # This allows to filter the input data when sql_executors are created
        pass

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        default_connection: Connection,
        mz_system_connection: Connection | None,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        return SqlExecutors(
            self.create_sql_executor(
                config, default_connection, mz_system_connection, output_printer, "mz"
            )
        )

    def create_sql_executor(
        self,
        config: ConsistencyTestConfiguration,
        default_connection: Connection,
        mz_system_connection: Connection | None,
        output_printer: OutputPrinter,
        name: str,
        is_mz: bool = True,
    ) -> SqlExecutor:
        if config.dry_run:
            return DryRunSqlExecutor(output_printer, name)

        if is_mz:
            return self.create_mz_sql_executor(
                config, default_connection, mz_system_connection, output_printer, name
            )

        return PgWireDatabaseSqlExecutor(
            default_connection, config.use_autocommit, output_printer, name
        )

    def create_mz_sql_executor(
        self,
        config: ConsistencyTestConfiguration,
        default_connection: Connection,
        mz_system_connection: Connection | None,
        output_printer: OutputPrinter,
        name: str,
    ) -> SqlExecutor:
        assert mz_system_connection is not None
        return MzDatabaseSqlExecutor(
            default_connection,
            mz_system_connection,
            config.use_autocommit,
            output_printer,
            name,
        )

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.OUTPUT_CONSISTENCY

    def create_result_comparator(
        self, ignore_filter: GenericInconsistencyIgnoreFilter
    ) -> ResultComparator:
        return ResultComparator(ignore_filter, ErrorMessageNormalizer())

    def create_inconsistency_ignore_filter(self) -> GenericInconsistencyIgnoreFilter:
        return InternalOutputInconsistencyIgnoreFilter()

    def create_evaluation_strategies(
        self, sql_executors: SqlExecutors
    ) -> list[EvaluationStrategy]:
        return [
            DataFlowRenderingEvaluation(),
            ConstantFoldingEvaluation(),
        ]


def upload_output_consistency_results_to_test_analytics(
    c: Composition,
    test_summary: ConsistencyTestSummary,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=test_summary.all_passed())

    test_analytics.output_consistency.add_stats(
        count_executed_queries=test_summary.count_executed_query_templates,
        count_successful_queries=test_summary.count_successful_query_templates,
        count_ignored_error_queries=test_summary.count_ignored_error_query_templates,
        count_failures=len(test_summary.failures),
        count_predefined_queries=test_summary.count_predefined_queries,
        count_available_data_types=test_summary.count_available_data_types,
        count_available_op_variants=test_summary.count_available_op_variants,
        count_used_ops=test_summary.count_used_ops(),
        count_generated_select_expressions=test_summary.count_generated_select_expressions,
        count_ignored_select_expressions=test_summary.count_ignored_select_expressions,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def connect(host: str, port: int, user: str, password: str | None = None) -> Connection:
    try:
        print(
            f"Connecting to database (host={host}, port={port}, user={user}, password={'****' if password else 'None'})"
        )
        return psycopg.connect(host=host, port=port, user=user, password=password)
    except OperationalError:
        print(f"Connecting to database failed (host={host}, port={port}, user={user})!")
        raise


def main() -> int:
    test = OutputConsistencyTest()
    parser = argparse.ArgumentParser(
        prog="output-consistency-test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the output consistency of different query evaluation strategies (e.g., dataflow rendering "
        "and constant folding).",
    )

    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6875, type=int)
    parser.add_argument("--system-port", default=6877, type=int)
    args = test.parse_output_consistency_input_args(parser)
    default_db_user = "materialize"
    mz_system_db_user = "mz_system"

    try:
        default_connection = connect(args.host, args.port, default_db_user)
        mz_system_connection = connect(args.host, args.system_port, mz_system_db_user)
    except OperationalError:
        return 1

    result = test.run_output_consistency_tests(
        default_connection,
        mz_system_connection,
        args,
        query_output_mode=QueryOutputMode.SELECT,
    )
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    exit(main())
