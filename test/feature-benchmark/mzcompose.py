# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
import time

from scenarios import *
from scipy import stats  # type: ignore

from materialize.feature_benchmark.aggregation import MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
)
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import NoFilter
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
)
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(
        validate_catalog=False,
        default_timeout="5m",
        depends_on=["kafka", "schema-registry", "materialized"],
    ),
]


def run_one_scenario(
    c: Composition, scenario: Scenario, this: Materialized, other: Materialized
) -> Comparator:
    name = scenario.__name__
    print(f"Now benchmarking {name} ...")
    comparator = RelativeThresholdComparator(name=name, threshold=0.10)
    common_seed = round(time.time())

    for mz_id, mz in enumerate([this, other]):
        with c.override(mz):
            c.up("materialized")

            executor = Docker(
                composition=c,
                seed=common_seed,
            )

            benchmark = Benchmark(
                mz_id=mz_id,
                scenario=scenario,
                executor=executor,
                filter=NoFilter(),
                termination_conditions=[
                    NormalDistributionOverlap(threshold=0.99),
                    ProbForMin(threshold=0.95),
                ],
                aggregation=MinAggregation(),
            )

            outcome, iterations = benchmark.run()
            comparator.append(outcome)

            c.rm("materialized")
            c.rm_volumes("mzdata")

    return comparator


def workflow_feature_benchmark(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--this-version", help="the version of materialized to test")
    parser.add_argument(
        "--this-options",
        default="",
        help="the command-line options to pass to this materialized",
    )
    parser.add_argument(
        "--other-version", help="the version of materialized to compare against"
    )
    parser.add_argument(
        "--other-options",
        default="",
        help="the command-line options to pass to the other materialized",
    )
    parser.add_argument(
        "scenario", nargs="?", default="Scenario", help="the scenario to run"
    )
    args = parser.parse_args()

    c.up("zookeeper", "kafka", "schema-registry")

    report = Report()
    has_regressions = False

    root_scenario = globals()[args.scenario]
    scenarios = []

    if root_scenario.__subclasses__():
        for scenario in root_scenario.__subclasses__():
            has_children = False
            for s in scenario.__subclasses__():
                has_children = True
                scenarios.append(s)

            if not has_children:
                scenarios.append(scenario)
    else:
        scenarios.append(root_scenario)

    print(
        f"will run the following scenarios: {', '.join([scenario.__name__ for scenario in scenarios])}"
    )

    this_image = None
    if args.this_version:
        this_image = f"materialize/materialized:{args.this_version}"

    other_image = None
    if args.other_version:
        other_image = f"materialize/materialized:{args.other_version}"

    for scenario in scenarios:
        comparison = run_one_scenario(
            c,
            scenario,
            Materialized(image=this_image, options=args.this_options),
            Materialized(image=other_image, options=args.other_options),
        )
        report.append(comparison)

        if comparison.is_regression():
            has_regressions = True

        report.dump()

    sys.exit(1 if has_regressions else 0)
