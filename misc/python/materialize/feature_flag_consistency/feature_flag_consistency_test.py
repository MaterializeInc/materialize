# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse
import time

from psycopg import Connection
from psycopg.errors import OperationalError

from materialize.feature_flag_consistency.execution.multi_config_executors import (
    MultiConfigSqlExecutors,
)
from materialize.feature_flag_consistency.feature_flag.feature_flag import (
    FeatureFlagSystemConfiguration,
    FeatureFlagSystemConfigurationPair,
    FeatureFlagValue,
)
from materialize.feature_flag_consistency.ignore_filter.feature_flag_consistency_ignore_filter import (
    FeatureFlagConsistencyIgnoreFilter,
)
from materialize.feature_flag_consistency.input_data.feature_flag_configurations import (
    FEATURE_FLAG_CONFIGURATION_PAIRS,
)
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
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.output_consistency_test import (
    OutputConsistencyTest,
    connect,
)


class FeatureFlagConsistencyTest(OutputConsistencyTest):

    def __init__(self) -> None:
        self.evaluation_strategy_name: str | None = None
        self.flag_configuration_pair: FeatureFlagSystemConfigurationPair | None = None
        self.mz2_connection: Connection | None = None
        self.mz2_system_connection: Connection | None = None

    def get_scenario(self) -> EvaluationScenario:
        return EvaluationScenario.FEATURE_FLAG_CONSISTENCY

    def create_evaluation_strategies(
        self, sql_executors: SqlExecutors
    ) -> list[EvaluationStrategy]:
        assert (
            self.evaluation_strategy_name is not None
        ), "Evaluation strategy name is not initialized"
        assert (
            self.flag_configuration_pair is not None
        ), "Configuration is not initialized"

        strategies = create_internal_evaluation_strategy_twice(
            self.evaluation_strategy_name
        )

        for strategy, flag_config in zip(
            strategies, self.flag_configuration_pair.get_configs()
        ):
            strategy.name = f"{strategy.name} {flag_config.name}"

            strategy.object_name_base = (
                f"{strategy.object_name_base}_{flag_config.shortcut}"
            )
            strategy.simple_db_object_name = (
                f"{strategy.simple_db_object_name}_{flag_config.shortcut}"
            )
            strategy.additional_setup_info = f"Config: {flag_config.to_system_params()}"

        return strategies

    def find_configuration_pair_by_name(
        self, name: str
    ) -> FeatureFlagSystemConfigurationPair:
        if name not in FEATURE_FLAG_CONFIGURATION_PAIRS.keys():
            raise RuntimeError(
                f"Feature flag configuration with name '{name}' not found. Available configuration names: {list(FEATURE_FLAG_CONFIGURATION_PAIRS.keys())}"
            )

        return FEATURE_FLAG_CONFIGURATION_PAIRS[name]

    def set_configuration_pair_by_name(self, name: str) -> None:
        self.flag_configuration_pair = self.find_configuration_pair_by_name(name)

    def create_inconsistency_ignore_filter(self) -> GenericInconsistencyIgnoreFilter:
        assert self.flag_configuration_pair is not None
        return FeatureFlagConsistencyIgnoreFilter(self.flag_configuration_pair)

    def create_sql_executors(
        self,
        config: ConsistencyTestConfiguration,
        default_connection: Connection,
        mz_system_connection: Connection,
        output_printer: OutputPrinter,
    ) -> SqlExecutors:
        assert (
            self.flag_configuration_pair is not None
        ), "Flag configuration is not initialized"
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

        return MultiConfigSqlExecutors(
            mz1_sql_executor, mz2_sql_executor, self.flag_configuration_pair
        )


def main() -> int:
    test = FeatureFlagConsistencyTest()
    parser = argparse.ArgumentParser(
        prog="feature-flag-consistency-test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Test the consistency of different feature flag configurations",
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
        "--query-output-mode",
        type=lambda mode: QueryOutputMode[mode.upper()],
        choices=QUERY_OUTPUT_MODE_CHOICES,
        default=QueryOutputMode.SELECT,
    )

    args = test.parse_output_consistency_input_args(parser)
    test.flag_configuration_pair = FeatureFlagSystemConfigurationPair(
        name="as_started",
        config1=FeatureFlagSystemConfiguration(
            name=f"as mz_1 running on {args.mz_port}",
            shortcut="as_mz_1",
            flags=[FeatureFlagValue("as_started", "mz-1")],
        ),
        config2=FeatureFlagSystemConfiguration(
            name=f"as mz_2 running on {args.mz_port_2}",
            shortcut="as_mz_2",
            flags=[FeatureFlagValue("as_started", "mz-2")],
        ),
    )

    print(
        "When running outside of mzcompose, make sure that the instances are started with the desired feature flags!"
    )
    time.sleep(5)

    try:
        mz_db_user = "materialize"
        my_system_user = "mz_system"
        mz_connection = connect(args.mz_host, args.mz_port, mz_db_user)
        mz_system_connection = connect(
            args.mz_host, args.mz_system_port, my_system_user
        )

        test.mz2_connection = connect(args.mz_host_2, args.mz_port_2, mz_db_user)
        test.mz2_system_connection = connect(
            args.mz_host_2, args.mz_system_port_2, my_system_user
        )

        test.evaluation_strategy_name = args.evaluation_strategy
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
