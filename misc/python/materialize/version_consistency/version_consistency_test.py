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
    ConstantFoldingEvaluation,
    DataFlowRenderingEvaluation,
    EvaluationStrategy,
    EvaluationStrategyKey,
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
from materialize.version_consistency.execution.multi_version_executors import (
    MultiVersionSqlExecutors,
)
from materialize.version_consistency.ignore_filter.version_consistency_ignore_filter import (
    VersionConsistencyIgnoreFilter,
)
from materialize.version_consistency.validation.version_consistency_error_message_normalizer import (
    VersionConsistencyErrorMessageNormalizer,
)

EVALUATION_STRATEGY_NAME_DFR = "dataflow_rendering"
EVALUATION_STRATEGY_NAME_CTF = "constant_folding"
EVALUATION_STRATEGY_NAMES = [EVALUATION_STRATEGY_NAME_DFR, EVALUATION_STRATEGY_NAME_CTF]


class VersionConsistencyTest(OutputConsistencyTest):
    def __init__(self) -> None:
        self.mz2_connection: Connection | None = None
        self.evaluation_strategy_name: str | None = None

    def shall_run(self, sql_executors: SqlExecutors) -> bool:
        assert isinstance(sql_executors, MultiVersionSqlExecutors)
        different_versions_involved = sql_executors.uses_different_versions()

        if not different_versions_involved:
            print("Involved versions are identical, aborting")

        return different_versions_involved

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.VERSION_CONSISTENCY

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        connection: Connection,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        assert self.mz2_connection is not None, "Second connection is not initialized"

        return MultiVersionSqlExecutors(
            create_sql_executor(config, connection, output_printer, "mz1"),
            create_sql_executor(config, self.mz2_connection, output_printer, "mz2"),
        )

    def create_result_comparator(
        self, ignore_filter: GenericInconsistencyIgnoreFilter
    ) -> ResultComparator:
        return ResultComparator(
            ignore_filter, VersionConsistencyErrorMessageNormalizer()
        )

    def create_inconsistency_ignore_filter(
        self, sql_executors: SqlExecutors
    ) -> GenericInconsistencyIgnoreFilter:
        assert isinstance(sql_executors, MultiVersionSqlExecutors)
        return VersionConsistencyIgnoreFilter(
            sql_executors.executor.query_version(),
            sql_executors.executor2.query_version(),
        )

    def create_evaluation_strategies(self) -> list[EvaluationStrategy]:
        assert (
            self.evaluation_strategy_name is not None
        ), "Evaluation strategy name is not initialized"

        strategies: list[EvaluationStrategy]

        if self.evaluation_strategy_name == EVALUATION_STRATEGY_NAME_DFR:
            strategies = [DataFlowRenderingEvaluation(), DataFlowRenderingEvaluation()]
            strategies[
                1
            ].identifier = EvaluationStrategyKey.MZ_DATAFLOW_RENDERING_OTHER_DB
        elif self.evaluation_strategy_name == EVALUATION_STRATEGY_NAME_CTF:
            strategies = [ConstantFoldingEvaluation(), ConstantFoldingEvaluation()]
            strategies[
                1
            ].identifier = EvaluationStrategyKey.MZ_CONSTANT_FOLDING_OTHER_DB
        else:
            raise RuntimeError(f"Unexpected name: {self.evaluation_strategy_name}")

        for i, strategy in enumerate(strategies):
            number = i + 1
            strategy.name = f"{strategy.name} {number}"
            strategy.object_name_base = f"{strategy.object_name_base}_{number}"
            strategy.simple_db_object_name = (
                f"{strategy.simple_db_object_name}_{number}"
            )

        return strategies


def main() -> int:
    test = VersionConsistencyTest()
    parser = argparse.ArgumentParser(
        prog="postgres-consistency-test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the consistency of different versions of mz",
    )

    parser.add_argument("--mz-host", default="localhost", type=str)
    parser.add_argument("--mz-port", default=6875, type=int)
    parser.add_argument("--mz-host-2", default="localhost", type=str)
    parser.add_argument("--mz-port-2", default=6975, type=int)
    parser.add_argument(
        "--evaluation-strategy",
        default=EVALUATION_STRATEGY_NAME_DFR,
        type=str,
        choices=EVALUATION_STRATEGY_NAMES,
    )

    args = test.parse_output_consistency_input_args(parser)

    try:
        mz_db_user = "materialize"
        mz_connection = connect(args.mz_host, args.mz_port, mz_db_user)
        test.mz2_connection = connect(args.mz_host_2, args.mz_port_2, mz_db_user)
        test.evaluation_strategy_name = args.evaluation_strategy
    except InterfaceError:
        return 1

    result = test.run_output_consistency_tests(mz_connection, args)
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    exit(main())
