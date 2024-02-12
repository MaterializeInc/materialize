# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

import pg8000
from pg8000 import Connection
from pg8000.exceptions import InterfaceError

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
)
from materialize.output_consistency.execution.sql_executor import create_sql_executor
from materialize.output_consistency.execution.sql_executors import SqlExecutors
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
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
from materialize.output_consistency.validation.result_comparator import ResultComparator


class OutputConsistencyTest:
    def run_output_consistency_tests(
        self,
        connection: Connection,
        args: argparse.Namespace,
    ) -> ConsistencyTestSummary:
        """Entry point for output consistency tests"""

        return self._run_output_consistency_tests_internal(
            connection,
            args.seed,
            args.dry_run,
            args.fail_fast,
            args.verbose,
            args.max_cols_per_query,
            args.max_runtime_in_sec,
            args.max_iterations,
            args.avoid_expressions_expecting_db_error,
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
        parser.add_argument(
            "--avoid-expressions-expecting-db-error",
            default=False,
            type=bool,
            action=argparse.BooleanOptionalAction,
        )

        return parser.parse_args()

    def _run_output_consistency_tests_internal(
        self,
        connection: Connection,
        random_seed: str,
        dry_run: bool,
        fail_fast: bool,
        verbose_output: bool,
        max_cols_per_query: int,
        max_runtime_in_sec: int,
        max_iterations: int,
        avoid_expressions_expecting_db_error: bool,
    ) -> ConsistencyTestSummary:
        input_data = ConsistencyTestInputData()

        output_printer = OutputPrinter(input_data)
        scenario = self.get_scenario()

        config = ConsistencyTestConfiguration(
            random_seed=random_seed,
            scenario=scenario,
            dry_run=dry_run,
            fail_fast=fail_fast,
            verbose_output=verbose_output,
            max_cols_per_query=max_cols_per_query,
            max_runtime_in_sec=max_runtime_in_sec,
            max_iterations=max_iterations,
            avoid_expressions_expecting_db_error=avoid_expressions_expecting_db_error,
            queries_per_tx=20,
            max_pending_expressions=100,
            use_autocommit=True,
            split_and_retry_on_db_error=True,
            print_reproduction_code=True,
            postgres_compatible_mode=scenario
            == EvaluationScenario.POSTGRES_CONSISTENCY,
        )

        output_printer.print_config(config)
        config.validate()

        if config.postgres_compatible_mode:
            input_data.remove_postgres_incompatible_data()

        evaluation_strategies = self.create_evaluation_strategies()

        randomized_picker = RandomizedPicker(config)

        sql_executors = self.create_sql_executors(config, connection, output_printer)

        ignore_filter = self.create_inconsistency_ignore_filter(sql_executors)

        expression_generator = ExpressionGenerator(
            config, randomized_picker, input_data
        )
        query_generator = QueryGenerator(
            config, randomized_picker, input_data, ignore_filter
        )
        output_comparator = self.create_result_comparator(ignore_filter)

        output_printer.print_info(sql_executors.get_database_infos())
        output_printer.print_empty_line()

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

        output_printer.start_section("Test execution")

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

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        connection: Connection,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        return SqlExecutors(
            create_sql_executor(config, connection, output_printer, "mz")
        )

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.OUTPUT_CONSISTENCY

    def create_result_comparator(
        self, ignore_filter: GenericInconsistencyIgnoreFilter
    ) -> ResultComparator:
        return ResultComparator(ignore_filter)

    def create_inconsistency_ignore_filter(
        self, sql_executors: SqlExecutors
    ) -> GenericInconsistencyIgnoreFilter:
        return InternalOutputInconsistencyIgnoreFilter()

    def create_evaluation_strategies(self) -> list[EvaluationStrategy]:
        return [
            DataFlowRenderingEvaluation(),
            ConstantFoldingEvaluation(),
        ]


def connect(host: str, port: int, user: str, password: str | None = None) -> Connection:
    try:
        print(
            f"Connecting to database (host={host}, port={port}, user={user}, password={'****' if password else 'None'})"
        )
        return pg8000.connect(host=host, port=port, user=user, password=password)
    except InterfaceError:
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
    args = test.parse_output_consistency_input_args(parser)
    db_user = "materialize"

    try:
        connection = connect(args.host, args.port, db_user)
    except InterfaceError:
        return 1

    result = test.run_output_consistency_tests(connection, args)
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    exit(main())
