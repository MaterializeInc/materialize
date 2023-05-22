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

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.common.format_constants import CONTENT_SEPARATOR_1
from materialize.output_consistency.execution.evaluation_strategy import (
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
)
from materialize.output_consistency.execution.sql_executor import create_sql_executor
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.generators.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.generators.query_generator import QueryGenerator
from materialize.output_consistency.known_inconsistencies.known_deviation_filter import (
    KnownOutputInconsistenciesFilter,
)
from materialize.output_consistency.runner.test_runner import ConsistencyTestRunner
from materialize.output_consistency.validation.result_comparator import ResultComparator


def run_output_consistency_tests(
    connection: Connection, args: argparse.Namespace
) -> ConsistencyTestSummary:
    """Entry point for output consistency tests"""

    return _run_output_consistency_tests_internal(
        connection,
        args.seed,
        args.dry_run,
        args.fail_fast,
        args.execute_setup,
        args.verbose,
        args.max_cols_per_query,
        args.runtime_in_sec,
        args.max_iterations,
        args.avoid_expressions_expecting_db_error,
    )


def parse_output_consistency_input_args(
    parser: argparse.ArgumentParser,
) -> argparse.Namespace:
    parser.add_argument("--seed", default="0", type=str)
    parser.add_argument(
        "--dry-run", default=False, type=bool, action=argparse.BooleanOptionalAction
    )
    parser.add_argument(
        "--fail-fast", default=False, type=bool, action=argparse.BooleanOptionalAction
    )
    parser.add_argument(
        "--execute-setup",
        default=True,
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
    parser.add_argument("--runtime-in-sec", default=600, type=int)
    parser.add_argument("--max-iterations", default=100000, type=int)
    parser.add_argument(
        "--avoid-expressions-expecting-db-error",
        default=False,
        type=bool,
        action=argparse.BooleanOptionalAction,
    )

    return parser.parse_args()


def _run_output_consistency_tests_internal(
    connection: Connection,
    random_seed: str,
    dry_run: bool,
    fail_fast: bool,
    execute_setup: bool,
    verbose_output: bool,
    max_cols_per_query: int,
    runtime_in_sec: int,
    max_iterations: int,
    avoid_expressions_expecting_db_error: bool,
) -> ConsistencyTestSummary:
    config = ConsistencyTestConfiguration()
    config.random_seed = random_seed
    config.dry_run = dry_run
    config.fail_fast = fail_fast
    config.execute_setup = execute_setup
    config.verbose_output = verbose_output
    config.max_cols_per_query = max_cols_per_query
    config.max_runtime_in_sec = runtime_in_sec
    config.max_iterations = max_iterations
    config.avoid_expressions_expecting_db_error = avoid_expressions_expecting_db_error

    print_config(config)

    evaluation_strategies = [
        DataFlowRenderingEvaluation(),
        ConstantFoldingEvaluation(),
    ]

    known_inconsistencies_filter = KnownOutputInconsistenciesFilter()

    expression_generator = ExpressionGenerator(config, known_inconsistencies_filter)
    query_generator = QueryGenerator(config)
    output_comparator = ResultComparator()
    sql_executor = create_sql_executor(config, connection)

    test_runner = ConsistencyTestRunner(
        config,
        evaluation_strategies,
        expression_generator,
        query_generator,
        output_comparator,
        sql_executor,
    )
    test_runner.setup()

    if not config.verbose_output:
        print(
            "Printing only queries with inconsistencies or warnings in non-verbose mode."
        )

    test_summary = test_runner.start()

    print(CONTENT_SEPARATOR_1)
    print(f"Test summary: {test_summary}")

    return test_summary


def print_config(config: ConsistencyTestConfiguration) -> None:
    config_properties = vars(config)
    print("Configuration is:")
    print("\n".join(f"  {item[0]} = {item[1]}" for item in config_properties.items()))


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="output-consistency",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the output consistency of different query evaluation strategies (e.g., dataflow rendering "
        "and constant folding).",
    )

    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6875, type=int)
    args = parse_output_consistency_input_args(parser)

    connection = pg8000.connect(host=args.host, port=args.port, user="materialize")

    result = run_output_consistency_tests(connection, args)
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    main()
