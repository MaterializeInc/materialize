# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse

from pg8000 import Connection
from pg8000.exceptions import InterfaceError

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
)
from materialize.output_consistency.execution.sql_executor import create_sql_executor
from materialize.output_consistency.execution.sql_executors import SqlExecutors
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.scenarios.evaluation_scenario import (
    EvaluationScenario,
)
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.output_consistency_test import (
    OutputConsistencyTest,
    connect,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.postgres_consistency.execution.pg_evaluation_strategy import (
    PgEvaluation,
)
from materialize.postgres_consistency.execution.pg_sql_executors import PgSqlExecutors
from materialize.postgres_consistency.ignore_filter.pg_inconsistency_ignore_filter import (
    PgInconsistencyIgnoreFilter,
)
from materialize.postgres_consistency.validation.pg_result_comparator import (
    PostgresResultComparator,
)


class PostgresConsistencyTest(OutputConsistencyTest):
    def __init__(self) -> None:
        self.pg_connection: Connection | None = None

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.POSTGRES_CONSISTENCY

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        connection: Connection,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        if self.pg_connection is None:
            raise RuntimeError("Postgres connection is not initialized")

        return PgSqlExecutors(
            create_sql_executor(config, connection, output_printer, "mz"),
            create_sql_executor(
                config, self.pg_connection, output_printer, "pg", is_mz=False
            ),
        )

    def create_result_comparator(
        self, ignore_filter: GenericInconsistencyIgnoreFilter
    ) -> ResultComparator:
        return PostgresResultComparator(ignore_filter)

    def create_inconsistency_ignore_filter(
        self, sql_executors: SqlExecutors
    ) -> GenericInconsistencyIgnoreFilter:
        return PgInconsistencyIgnoreFilter()

    def create_evaluation_strategies(self) -> list[EvaluationStrategy]:
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

    parser.add_argument("--mz-host", default="localhost", type=str)
    parser.add_argument("--mz-port", default=6875, type=int)
    parser.add_argument("--pg-host", default="localhost", type=str)
    parser.add_argument("--pg-port", default=5432, type=int)
    parser.add_argument("--pg-password", default=None, type=str)
    args = test.parse_output_consistency_input_args(parser)

    try:
        mz_db_user = "materialize"
        mz_connection = connect(args.mz_host, args.mz_port, mz_db_user)

        pg_db_user = "postgres"
        test.pg_connection = connect(
            args.pg_host, args.pg_port, pg_db_user, args.pg_password
        )
    except InterfaceError:
        return 1

    result = test.run_output_consistency_tests(mz_connection, args)
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    exit(main())
