# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.output_consistency.configuration.output_consistency_configuration import (
    DEFAULT_CONFIG,
    OutputConsistencyConfiguration,
)
from materialize.output_consistency.data_type.data_provider import DATA_TYPES
from materialize.output_consistency.execution.evaluation_strategy import (
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
)
from materialize.output_consistency.execution.query_executor import QueryExecutor
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.expressions.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.query.query_generator import QueryGenerator
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker
from materialize.output_consistency.validation.result_comparator import ResultComparator


def run_output_consistency_tests(c: Composition) -> ConsistencyTestSummary:
    config: OutputConsistencyConfiguration = DEFAULT_CONFIG

    evaluation_strategies = [
        DataFlowRenderingEvaluation(),
        ConstantFoldingEvaluation(),
    ]

    data_generator = ExpressionGenerator()
    expressions = data_generator.generate_expressions()
    print(f"Created {len(expressions)} expressions.")

    randomized_picker = RandomizedPicker(config)
    expressions = randomized_picker.select(expressions, num_elements=10)
    print(f"Selected {len(expressions)} expressions.")

    query_generator = QueryGenerator(config)
    queries = query_generator.generate_queries(expressions)
    print(f"Created {len(queries)} queries.")

    comparator = ResultComparator()

    executor = QueryExecutor(evaluation_strategies, config, comparator)
    executor.setup_database_objects(c, DATA_TYPES, evaluation_strategies)
    test_summary = executor.execute_queries(c, queries)

    print(f"Test summary: {test_summary}")

    return test_summary
