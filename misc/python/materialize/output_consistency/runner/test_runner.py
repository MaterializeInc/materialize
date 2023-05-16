# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.data_type.data_provider import DATA_TYPES
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.query_execution_manager import (
    QueryExecutionManager,
)
from materialize.output_consistency.execution.sql_executor import SqlExecutor
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.expressions.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.query.query_generator import QueryGenerator
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker
from materialize.output_consistency.validation.result_comparator import ResultComparator


class ConsistencyTestRunner:
    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        evaluation_strategies: list[EvaluationStrategy],
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
            DATA_TYPES, self.evaluation_strategies
        )

    def start(self) -> ConsistencyTestSummary:
        randomized_picker = RandomizedPicker(self.config)

        expressions = self.expression_generator.generate_expressions()
        print(f"Created {len(expressions)} expressions.")

        num_expressions_to_select = 20
        expressions = randomized_picker.select(
            expressions, num_elements=num_expressions_to_select
        )
        print(f"Selected {len(expressions)} expressions.")

        queries = self.query_generator.generate_queries(expressions)
        print(f"Created {len(queries)} queries.")

        test_summary = self.execution_manager.execute_queries(queries)

        return test_summary
