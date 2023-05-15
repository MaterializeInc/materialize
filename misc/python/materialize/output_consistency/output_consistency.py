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
from pg8000 import Cursor

from materialize.output_consistency.common.configuration import (
    DEFAULT_CONFIG,
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.common.format_constants import CONTENT_SEPARATOR_1
from materialize.output_consistency.data_type.data_provider import DATA_TYPES
from materialize.output_consistency.execution.evaluation_strategy import (
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
)
from materialize.output_consistency.execution.query_execution_manager import (
    QueryExecutionManager,
)
from materialize.output_consistency.execution.sql_executor import create_sql_executor
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.expressions.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.query.query_generator import QueryGenerator
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker
from materialize.output_consistency.validation.result_comparator import ResultComparator


def run_output_consistency_tests(
    cursor: Cursor,
    runtime: int,
    random_seed: int,
    dry_run: bool,
    fail_fast: bool,
    execute_setup: bool,
    verbose_output: bool,
) -> ConsistencyTestSummary:
    config: ConsistencyTestConfiguration = DEFAULT_CONFIG
    config.random_seed = random_seed
    config.dry_run = dry_run
    config.fail_fast = fail_fast
    config.execute_setup = execute_setup
    config.verbose_output = verbose_output
    num_expressions_to_select = 20

    evaluation_strategies = [
        DataFlowRenderingEvaluation(),
        ConstantFoldingEvaluation(),
    ]

    data_generator = ExpressionGenerator()
    expressions = data_generator.generate_expressions()
    print(f"Created {len(expressions)} expressions.")

    randomized_picker = RandomizedPicker(config)
    expressions = randomized_picker.select(
        expressions, num_elements=num_expressions_to_select
    )
    print(f"Selected {len(expressions)} expressions.")

    query_generator = QueryGenerator(config)
    queries = query_generator.generate_queries(expressions)
    print(f"Created {len(queries)} queries.")

    comparator = ResultComparator()
    sql_executor = create_sql_executor(config, cursor)

    execution_manager = QueryExecutionManager(
        evaluation_strategies, config, sql_executor, comparator
    )
    execution_manager.setup_database_objects(DATA_TYPES, evaluation_strategies)

    if not config.verbose_output:
        print("Printing only queries with inconsistencies or warnings in non-verbose mode.")

    test_summary = execution_manager.execute_queries(queries)

    print(CONTENT_SEPARATOR_1)
    print(f"Test summary: {test_summary}")

    return test_summary


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="output-consistency",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the output consistency of different query evaluation strategies (e.g., dataflow rendering and constant folding).",
    )

    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--seed", default=0, type=int)
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
    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6875, type=int)
    args = parser.parse_args()

    conn = pg8000.connect(host=args.host, port=args.port, user="materialize")
    conn.autocommit = True
    cursor = conn.cursor()

    result = run_output_consistency_tests(
        cursor,
        args.runtime,
        args.seed,
        args.dry_run,
        args.fail_fast,
        args.execute_setup,
        args.verbose,
    )
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    main()
