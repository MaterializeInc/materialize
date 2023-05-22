# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from datetime import datetime, timedelta
from typing import List

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.query_execution_manager import (
    QueryExecutionManager,
)
from materialize.output_consistency.execution.sql_executor import SqlExecutor
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.generators.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.generators.query_generator import QueryGenerator
from materialize.output_consistency.input_data.values.all_values_provider import (
    ALL_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator


class ConsistencyTestRunner:
    """Orchestrates the test execution"""

    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        evaluation_strategies: List[EvaluationStrategy],
        expression_generator: ExpressionGenerator,
        query_generator: QueryGenerator,
        output_comparator: ResultComparator,
        sql_executor: SqlExecutor,
    ):
        self.config = config
        self.evaluation_strategies = evaluation_strategies
        self.expression_generator = expression_generator
        self.query_generator = query_generator
        self.output_comparator = output_comparator
        self.sql_executor = sql_executor
        self.execution_manager = QueryExecutionManager(
            evaluation_strategies, config, sql_executor, output_comparator
        )

    def setup(self) -> None:
        self.execution_manager.setup_database_objects(
            ALL_DATA_TYPES_WITH_VALUES, self.evaluation_strategies
        )

    def start(self) -> ConsistencyTestSummary:
        expression_count = 0
        test_summary = ConsistencyTestSummary(dry_run=self.config.dry_run)

        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=self.config.max_runtime_in_sec)

        while True:
            if expression_count > 0 and expression_count % 200 == 0:
                print(f"Status: Expression {expression_count}...")

            operation = self.expression_generator.pick_random_operation()

            shall_abort_after_iteration = self._shall_abort(expression_count, end_time)

            expression = self.expression_generator.generate_expression(
                operation, test_summary
            )

            if expression is None:
                test_summary.global_warnings.append(
                    f"Failed to generate an expression for operation {operation}!"
                )
                continue

            self.query_generator.push_expression(expression)

            if self.query_generator.shall_consume_query() or (
                shall_abort_after_iteration and self.query_generator.can_consume_query()
            ):
                query = self.query_generator.consume_query()
                success = self.execution_manager.execute_query(query, test_summary)

                if not success and self.config.fail_fast:
                    print(
                        "Ending test run because the first comparison mismatch has occurred (fail_fast mode)"
                    )
                    shall_abort_after_iteration = True

            expression_count += 1

            if shall_abort_after_iteration:
                break

        self.execution_manager.complete()

        return test_summary

    def _shall_abort(self, iteration_count: int, end_time: datetime) -> bool:
        if iteration_count >= self.config.max_iterations:
            print("Ending test run because the iteration count limit has been reached")
            return True

        if datetime.now() >= end_time:
            print("Ending test run because the maximum runtime has been reached")
            return True

        return False
