# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse

from psycopg import Connection
from psycopg.errors import OperationalError

from materialize.mz_version import MzVersion
from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EVALUATION_STRATEGY_NAME_DFR,
    INTERNAL_EVALUATION_STRATEGY_NAMES,
    EvaluationStrategy,
    create_internal_evaluation_strategy_twice,
)
from materialize.output_consistency.execution.query_output_mode import (
    QUERY_OUTPUT_MODE_CHOICES,
    QueryOutputMode,
)
from materialize.output_consistency.execution.sql_executors import SqlExecutors
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.scenarios.evaluation_scenario import (
    EvaluationScenario,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.output_consistency_test import (
    OutputConsistencyTest,
    connect,
)
from materialize.version_consistency.execution.multi_version_executors import (
    MultiVersionSqlExecutors,
)
from materialize.version_consistency.ignore_filter.version_consistency_ignore_filter import (
    VersionConsistencyIgnoreFilter,
)


class VersionConsistencyTest(OutputConsistencyTest):
    def __init__(self) -> None:
        self.mz2_connection: Connection | None = None
        self.mz2_system_connection: Connection | None = None
        self.evaluation_strategy_name: str | None = None
        self.allow_same_version_comparison = False
        # values will be available after create_sql_executors is called
        self.mz1_version_without_dev_suffix: MzVersion | None = None
        self.mz2_version_without_dev_suffix: MzVersion | None = None

    def shall_run(self, sql_executors: SqlExecutors) -> bool:
        assert isinstance(sql_executors, MultiVersionSqlExecutors)
        different_versions_involved = sql_executors.uses_different_versions()

        if not different_versions_involved:
            if self.allow_same_version_comparison:
                print(
                    "Involved versions are identical, but continuing due to allow_same_version_comparison"
                )
                return True
            else:
                print("Involved versions are identical, aborting")

        return different_versions_involved

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.VERSION_CONSISTENCY

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        default_connection: Connection,
        mz_system_connection: Connection,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        assert self.mz2_connection is not None, "Second connection is not initialized"
        assert (
            self.mz2_system_connection is not None
        ), "Second system connection is not initialized"

        mz1_sql_executor = self.create_sql_executor(
            config, default_connection, mz_system_connection, output_printer, "mz1"
        )
        mz2_sql_executor = self.create_sql_executor(
            config,
            self.mz2_connection,
            self.mz2_system_connection,
            output_printer,
            "mz2",
        )

        self.mz1_version_without_dev_suffix = MzVersion.parse_mz(
            mz1_sql_executor.query_version(), drop_dev_suffix=True
        )
        self.mz2_version_without_dev_suffix = MzVersion.parse_mz(
            mz2_sql_executor.query_version(), drop_dev_suffix=True
        )

        return MultiVersionSqlExecutors(
            mz1_sql_executor,
            mz2_sql_executor,
        )

    def create_inconsistency_ignore_filter(self) -> GenericInconsistencyIgnoreFilter:
        assert self.mz1_version_without_dev_suffix is not None
        assert self.mz2_version_without_dev_suffix is not None

        assert (
            self.evaluation_strategy_name is not None
        ), "Evaluation strategy name is not initialized"

        uses_dfr = self.evaluation_strategy_name == EVALUATION_STRATEGY_NAME_DFR
        return VersionConsistencyIgnoreFilter(
            self.mz1_version_without_dev_suffix,
            self.mz2_version_without_dev_suffix,
            uses_dfr,
        )

    def create_evaluation_strategies(
        self, sql_executors: SqlExecutors
    ) -> list[EvaluationStrategy]:
        assert (
            self.evaluation_strategy_name is not None
        ), "Evaluation strategy name is not initialized"

        strategies = create_internal_evaluation_strategy_twice(
            self.evaluation_strategy_name
        )

        for i, strategy in enumerate(strategies):
            number = i + 1
            sql_executor = sql_executors.get_executor(strategy)

            version = sql_executor.query_version()
            sanitized_version_string = sanitize_and_shorten_version_string(version)

            strategy.name = f"{strategy.name} {version}"

            # include the number as well since the short version string may not be unique
            strategy.object_name_base = (
                f"{strategy.object_name_base}_{number}_{sanitized_version_string}"
            )
            strategy.simple_db_object_name = (
                f"{strategy.simple_db_object_name}_{number}_{sanitized_version_string}"
            )

        return strategies

    def filter_input_data(self, input_data: ConsistencyTestInputData) -> None:
        input_data.operations_input.remove_functions(
            self._is_operation_unsupported_in_any_versions
        )

    def _is_operation_unsupported_in_any_versions(
        self, operation: DbOperationOrFunction
    ) -> bool:
        if operation.since_mz_version is None:
            return False

        assert self.mz1_version_without_dev_suffix is not None
        assert self.mz2_version_without_dev_suffix is not None

        return (
            operation.since_mz_version > self.mz1_version_without_dev_suffix
            or operation.since_mz_version > self.mz2_version_without_dev_suffix
        )


def sanitize_and_shorten_version_string(version: str) -> str:
    """
    Drop the commit hash and replace dots and dashes with an underscore
    :param version: looks like "v0.98.5 (4cfc26688)", version may contain a "-dev" suffix
    """

    mz_version = MzVersion.parse(version)
    return str(mz_version).replace(".", "_").replace("-", "_")


def main() -> int:
    test = VersionConsistencyTest()
    parser = argparse.ArgumentParser(
        prog="version-consistency-test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the consistency of different versions of mz",
    )

    parser.add_argument("--mz-host", default="localhost", type=str)
    parser.add_argument("--mz-port", default=6875, type=int)
    parser.add_argument("--mz-system-port", default=6877, type=int)
    parser.add_argument("--mz-host-2", default="localhost", type=str)
    parser.add_argument("--mz-port-2", default=6975, type=int)
    parser.add_argument("--mz-system-port-2", default=6977, type=int)
    parser.add_argument(
        "--evaluation-strategy",
        default=EVALUATION_STRATEGY_NAME_DFR,
        type=str,
        choices=INTERNAL_EVALUATION_STRATEGY_NAMES,
    )
    parser.add_argument(
        "--other-tag",
        type=str,
        default="common-ancestor",
    )
    parser.add_argument(
        "--allow-same-version-comparison",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--query-output-mode",
        type=lambda mode: QueryOutputMode[mode.upper()],
        choices=QUERY_OUTPUT_MODE_CHOICES,
        default=QueryOutputMode.SELECT,
    )

    args = test.parse_output_consistency_input_args(parser)

    try:
        mz_db_user = "materialize"
        mz_system_user = "mz_system"
        mz_connection = connect(args.mz_host, args.mz_port, mz_db_user)
        mz_system_connection = connect(
            args.mz_host, args.mz_system_port, mz_system_user
        )
        test.mz2_connection = connect(args.mz_host_2, args.mz_port_2, mz_db_user)
        test.mz2_system_connection = connect(
            args.mz_host_2, args.mz_system_port_2, mz_system_user
        )
        test.evaluation_strategy_name = args.evaluation_strategy
        test.allow_same_version_comparison = args.allow_same_version_comparison
    except OperationalError:
        return 1

    result = test.run_output_consistency_tests(
        mz_connection,
        mz_system_connection,
        args,
        query_output_mode=args.query_output_mode,
    )
    return 0 if result.all_passed() else 1


if __name__ == "__main__":
    exit(main())
