# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse
from typing import List

from pg8000.exceptions import InterfaceError

from materialize.output_consistency.execution.evaluation_strategy import (
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
)
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    InconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.scenarios.evaluation_scenario import (
    EvaluationScenario,
)
from materialize.output_consistency.output_consistency_test import (
    OutputConsistencyTest,
    connect,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.postgres_consistency.execution.pg_evaluation_strategy import (
    PgEvaluation,
)
from materialize.postgres_consistency.ignore_filter.pg_inconsistency_ignore_filter import (
    PgInconsistencyIgnoreFilter,
)
from materialize.postgres_consistency.validation.pg_result_comparator import (
    PostgresResultComparator,
)


class PostgresConsistencyTest(OutputConsistencyTest):
    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.POSTGRES_CONSISTENCY

    def create_result_comparator(
        self, ignore_filter: InconsistencyIgnoreFilter
    ) -> ResultComparator:
        return PostgresResultComparator(ignore_filter)

    def create_inconsistency_ignore_filter(self) -> InconsistencyIgnoreFilter:
        return PgInconsistencyIgnoreFilter()

    def create_evaluation_strategies(self) -> List[EvaluationStrategy]:
        mz_evaluation_strategy = DataFlowRenderingEvaluation()
        mz_evaluation_strategy.name = "Materialize evaluation"
        mz_evaluation_strategy.simple_db_object_name = "mz_evaluation"
        return [
            # Materialize
            mz_evaluation_strategy,
            # Postgres
            PgEvaluation(),
        ]


def main() -> int:
    test = PostgresConsistencyTest()
    parser = argparse.ArgumentParser(
        prog="postgres-consistency-test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the consistency of Materialize and Postgres",
    )

    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--mz-port", default=6875, type=int)
    args = test.parse_output_consistency_input_args(parser)
    db_user = "materialize"

    try:
        mz_connection = connect(args.host, args.mz_port, db_user)
    except InterfaceError:
        return 1

    result = test.run_output_consistency_tests(mz_connection, args)
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    main()
