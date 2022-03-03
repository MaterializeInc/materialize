# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import sys
import time
from typing import List, Type

# mzcompose may start this script from the root of the Mz repository,
# so we need to explicitly add this directory to the Python module search path
sys.path.append(os.path.dirname(__file__))
from scenarios import *

from materialize.feature_benchmark.aggregation import Aggregation, MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
)
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import Filter, FilterFirst, NoFilter
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
    RunAtMost,
    TerminationCondition,
)
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Kafka as KafkaService
from materialize.mzcompose.services import Kgen as KgenService
from materialize.mzcompose.services import (
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

#
# Global feature benchmark thresholds and termination conditions
#


def make_filter(args: argparse.Namespace) -> Filter:
    # Discard the first run unless a small --max-runs limit is explicitly set
    if args.max_runs <= 5:
        return NoFilter()
    else:
        return FilterFirst()


def make_termination_conditions(args: argparse.Namespace) -> List[TerminationCondition]:
    return [
        NormalDistributionOverlap(threshold=0.99),
        ProbForMin(threshold=0.95),
        RunAtMost(threshold=args.max_runs),
    ]


def make_aggregation() -> Aggregation:
    return MinAggregation()


def make_comparator(name: str) -> Comparator:
    return RelativeThresholdComparator(name, threshold=0.10)


default_timeout = "30m"

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    # We are going to override this service definition during the actual benchmark
    # we put "latest" here so that we avoid recompiling the current source unless
    # we will actually be benchmarking it.
    Materialized(image="materialize/materialized:latest"),
    Testdrive(
        validate_catalog=False,
        default_timeout=default_timeout,
    ),
    KgenService(),
    Postgres(),
]


def run_one_scenario(
    c: Composition, scenario: Type[Scenario], args: argparse.Namespace
) -> Comparator:
    name = scenario.__name__
    print(f"--- Now benchmarking {name} ...")
    comparator = make_comparator(name)
    common_seed = round(time.time())

    mzs = {
        "this": Materialized(
            image=f"materialize/materialized:{args.this_tag}"
            if args.this_tag
            else None,
            options=args.this_options,
        ),
        "other": Materialized(
            image=f"materialize/materialized:{args.other_tag}"
            if args.other_tag
            else None,
            options=args.other_options,
        ),
    }

    for mz_id, instance in enumerate(["this", "other"]):
        with c.override(mzs[instance]):
            print(f"The version of the '{instance.upper()}' Mz instance is:")
            c.run("materialized", "--version")

            c.start_and_wait_for_tcp(services=["materialized"])
            c.wait_for_materialized()

            executor = Docker(
                composition=c,
                seed=common_seed,
            )

            benchmark = Benchmark(
                mz_id=mz_id,
                scenario=scenario,
                scale=args.scale,
                executor=executor,
                filter=make_filter(args),
                termination_conditions=make_termination_conditions(args),
                aggregation=make_aggregation(),
            )

            outcome, iterations = benchmark.run()
            comparator.append(outcome)

            c.kill("materialized")
            c.rm("materialized", "testdrive-svc")
            c.rm_volumes("mzdata")

    return comparator


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Feature benchmark framework."""

    c.silent = True

    parser.add_argument(
        "--this-tag",
        metavar="TAG",
        type=str,
        default=os.getenv("THIS_TAG", None),
        help="'This' Materialize container tag to benchmark. If not provided, the current source will be used.",
    )

    parser.add_argument(
        "--this-options",
        metavar="OPTIONS",
        type=str,
        default=os.getenv("THIS_OPTIONS", None),
        help="Options to pass to the 'This' instance.",
    )

    parser.add_argument(
        "--other-tag",
        metavar="TAG",
        type=str,
        default=os.getenv("OTHER_TAG", None),
        help="'Other' Materialize container tag to benchmark. If not provided, the current source will be used.",
    )

    parser.add_argument(
        "--other-options",
        metavar="OPTIONS",
        type=str,
        default=os.getenv("OTHER_OPTIONS", None),
        help="Options to pass to the 'Other' instance.",
    )

    parser.add_argument(
        "--root-scenario",
        "--scenario",
        metavar="SCENARIO",
        type=str,
        default="Scenario",
        help="Scenario or scenario family to benchmark. See scenarios.py for available scenarios.",
    )

    parser.add_argument(
        "--scale",
        metavar="+N | -N | N",
        type=str,
        default=None,
        help="Absolute or relative scale to apply.",
    )

    parser.add_argument(
        "--max-runs",
        metavar="N",
        type=int,
        default=99,
        help="Limit the number of executions to N.",
    )

    args = parser.parse_args()

    print(
        f"""
this_tag: {args.this_tag}
this_options: {args.this_options}

other_tag: {args.other_tag}
other_options: {args.other_options}

root_scenario: {args.root_scenario}"""
    )

    # Build the list of scenarios to run
    root_scenario = globals()[args.root_scenario]
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

    print(f"scenarios: {', '.join([scenario.__name__ for scenario in scenarios])}")

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "postgres"]
    )

    report = Report()
    has_regressions = False

    for scenario in scenarios:
        comparison = run_one_scenario(c, scenario, args)
        report.append(comparison)

        if comparison.is_regression():
            has_regressions = True
        report.dump()

    print("+++ Benchmark Report")
    report.dump()

    if has_regressions:
        print("ERROR: benchmarks have regressions")
        sys.exit(1)
