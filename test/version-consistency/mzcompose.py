# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from random import Random

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_consistency.version_consistency_test import (
    EVALUATION_STRATEGY_NAMES,
    VersionConsistencyTest,
)
from materialize.version_list import (
    resolve_ancestor_image_tag,
)

SERVICES = [
    Cockroach(setup_materialize=True),
    Postgres(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test the consistency with another mz version.
    """

    c.down(destroy_volumes=True)

    test = VersionConsistencyTest()

    evaluation_strategy_name_random = "random"
    extended_evaluation_strategy_names = EVALUATION_STRATEGY_NAMES + [
        evaluation_strategy_name_random
    ]
    parser.add_argument(
        "--evaluation-strategy",
        default=evaluation_strategy_name_random,
        type=str,
        choices=extended_evaluation_strategy_names,
    )

    args = test.parse_output_consistency_input_args(parser)

    if args.evaluation_strategy == evaluation_strategy_name_random:
        evaluation_strategy_name = Random(args.seed).choice(EVALUATION_STRATEGY_NAMES)
    else:
        evaluation_strategy_name = args.evaluation_strategy

    name_mz_this, name_mz_other = "mz_this", "mz_other"
    port_mz_internal, port_mz_this, port_mz_other = 6875, 6875, 16875
    tag_mz_other = resolve_ancestor_image_tag(
        ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
    )

    print(f"Using {tag_mz_other} as tag for other mz version")

    with c.override(
        Materialized(
            name=name_mz_this,
            image=None,
            ports=[f"{port_mz_this}:{port_mz_internal}"],
            use_default_volumes=False,
        ),
        Materialized(
            name=name_mz_other,
            image=f"materialize/materialized:{tag_mz_other}",
            ports=[f"{port_mz_other}:{port_mz_internal}"],
            use_default_volumes=False,
        ),
    ):
        c.up(name_mz_this)
        c.up(name_mz_other)

        connection = c.sql_connection(service=name_mz_this, port=port_mz_internal)
        test.mz2_connection = c.sql_connection(
            service=name_mz_other, port=port_mz_internal
        )
        test.evaluation_strategy_name = evaluation_strategy_name

        test_summary = test.run_output_consistency_tests(connection, args)

    if not test_summary.all_passed():
        raise FailedTestExecutionError(
            "At least one test failed", errors=test_summary.failures
        )
