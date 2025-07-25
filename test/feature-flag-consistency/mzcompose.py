# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verify that Materialize has the same results with different sets of feature flags.
"""

from materialize.feature_flag_consistency.feature_flag_consistency_test import (
    FeatureFlagConsistencyTest,
)
from materialize.feature_flag_consistency.input_data.feature_flag_configurations import (
    FEATURE_FLAG_CONFIGURATION_PAIRS,
)
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.output_consistency.execution.evaluation_strategy import (
    EVALUATION_STRATEGY_NAME_DFR,
    INTERNAL_EVALUATION_STRATEGY_NAMES,
)
from materialize.output_consistency.execution.query_output_mode import (
    QUERY_OUTPUT_MODE_CHOICES,
    QueryOutputMode,
)
from materialize.output_consistency.output_consistency_test import (
    upload_output_consistency_results_to_test_analytics,
)

SERVICES = [
    Cockroach(setup_materialize=True),
    Postgres(),
    Materialized(name="mz_this"),  # Overridden below
    Materialized(name="mz_other"),  # Overridden below
    Mz(app_password=""),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test the consistency with another mz version.
    """

    c.down(destroy_volumes=True)

    test = FeatureFlagConsistencyTest()

    parser.add_argument("--configuration", action="append", default=[], type=str)

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
        "--query-output-mode",
        type=lambda mode: QueryOutputMode[mode.upper()],
        choices=QUERY_OUTPUT_MODE_CHOICES,
        default=QueryOutputMode.SELECT,
    )

    args = test.parse_output_consistency_input_args(parser)

    port_mz_default_internal, port_mz_system_internal = 6875, 6877
    configurations = args.configuration

    if len(configurations) == 0:
        configurations = FEATURE_FLAG_CONFIGURATION_PAIRS.keys()

    runtime_per_config_in_sec = args.max_runtime_in_sec / len(configurations)

    test_summaries = []

    for configuration in configurations:
        test.set_configuration_pair_by_name(configuration)
        assert test.flag_configuration_pair is not None
        print(f"Running flag configuration: {test.flag_configuration_pair.name}")

        with c.override(
            Materialized(
                name="mz_this",
                additional_system_parameter_defaults=test.flag_configuration_pair.config1.to_system_params(),
                use_default_volumes=False,
            ),
            Materialized(
                name="mz_other",
                ports=[16875, 16876, 16877, 16878, 16879],
                additional_system_parameter_defaults=test.flag_configuration_pair.config2.to_system_params(),
                use_default_volumes=False,
            ),
        ):
            c.up("mz_this", "mz_other")

            default_connection = c.sql_connection(
                service="mz_this", port=port_mz_default_internal
            )
            mz_system_connection = c.sql_connection(
                service="mz_this", port=port_mz_system_internal, user="mz_system"
            )
            test.mz2_connection = c.sql_connection(
                service="mz_other", port=port_mz_default_internal
            )
            test.mz2_system_connection = c.sql_connection(
                service="mz_other", port=port_mz_system_internal, user="mz_system"
            )

            test.evaluation_strategy_name = args.evaluation_strategy

            test_summary = test.run_output_consistency_tests(
                default_connection,
                mz_system_connection,
                args,
                query_output_mode=args.query_output_mode,
                override_max_runtime_in_sec=runtime_per_config_in_sec,
            )

            test_summaries.append(test_summary)

        assert len(test_summaries) > 0

        merged_summary = test_summaries[0]

        for test_summary in test_summaries[1:]:
            merged_summary.merge(test_summary)

        upload_output_consistency_results_to_test_analytics(c, merged_summary)

        if not merged_summary.all_passed():
            raise FailedTestExecutionError(errors=merged_summary.failures)
