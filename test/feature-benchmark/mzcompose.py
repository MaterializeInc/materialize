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
import uuid
from textwrap import dedent
from typing import List, Type

# mzcompose may start this script from the root of the Mz repository,
# so we need to explicitly add this directory to the Python module search path
sys.path.append(os.path.dirname(__file__))
from scenarios import *  # noqa: F401 F403
from scenarios import Scenario
from scenarios_concurrency import *  # noqa: F401 F403
from scenarios_customer import *  # noqa: F401 F403
from scenarios_optbench import *  # noqa: F401 F403
from scenarios_scale import *  # noqa: F401 F403
from scenarios_subscribe import *  # noqa: F401 F403

from materialize.feature_benchmark.aggregation import Aggregation, MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
)
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import Filter, FilterFirst, NoFilter
from materialize.feature_benchmark.measurement import MeasurementType
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
    RunAtMost,
    TerminationCondition,
)
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Cockroach
from materialize.mzcompose.services import Kafka as KafkaService
from materialize.mzcompose.services import Kgen as KgenService
from materialize.mzcompose.services import (
    Materialized,
    Minio,
    Postgres,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.version_list import VersionsFromDocs

#
# Global feature benchmark thresholds and termination conditions
#


def make_filter(args: argparse.Namespace) -> Filter:
    # Discard the first run unless a small --max-runs limit is explicitly set
    if args.max_measurements <= 5:
        return NoFilter()
    else:
        return FilterFirst()


def make_termination_conditions(args: argparse.Namespace) -> List[TerminationCondition]:
    return [
        NormalDistributionOverlap(threshold=0.99),
        ProbForMin(threshold=0.95),
        RunAtMost(threshold=args.max_measurements),
    ]


def make_aggregation_class() -> Type[Aggregation]:
    return MinAggregation


def make_comparator(name: str, type: MeasurementType) -> Comparator:
    return RelativeThresholdComparator(name=name, type=type, threshold=0.10)


default_timeout = "1800s"

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    Redpanda(),
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    Materialized(),
    Testdrive(
        default_timeout=default_timeout,
        materialize_params={"statement_timeout": f"'{default_timeout}'"},
    ),
    KgenService(),
    Postgres(),
]


def run_one_scenario(
    c: Composition, scenario: Type[Scenario], args: argparse.Namespace
) -> List[Comparator]:
    name = scenario.__name__
    print(f"--- Now benchmarking {name} ...")

    measurement_types = [MeasurementType.WALLCLOCK]
    if args.measure_memory:
        measurement_types.append(MeasurementType.MEMORY)
    comparators = [make_comparator(name=name, type=t) for t in measurement_types]

    common_seed = round(time.time())

    for mz_id, instance in enumerate(["this", "other"]):
        tag, size, params = (
            (args.this_tag, args.this_size, args.this_params)
            if instance == "this"
            else (args.other_tag, args.other_size, args.other_params)
        )

        c.up("testdrive", persistent=True)

        additional_system_parameter_defaults = None
        if params is not None:
            additional_system_parameter_defaults = {}
            for param in params.split(";"):
                param_name, param_value = param.split("=")
                additional_system_parameter_defaults[param_name] = param_value

        mz = Materialized(
            image=f"materialize/materialized:{tag}" if tag else None,
            default_size=size,
            # Avoid clashes with the Kafka sink progress topic across restarts
            environment_id=f"local-az1-{uuid.uuid4()}-0",
            soft_assertions=False,
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            external_cockroach=True,
            external_minio=True,
        )

        with c.override(mz):
            print(f"The version of the '{instance.upper()}' Mz instance is:")
            c.run(
                "materialized",
                "-c",
                "environmentd --version | grep environmentd",
                entrypoint="bash",
                rm=True,
            )

            c.up("cockroach", "materialized")

        executor = Docker(composition=c, seed=common_seed, materialized=mz)

        benchmark = Benchmark(
            mz_id=mz_id,
            scenario=scenario,
            scale=args.scale,
            executor=executor,
            filter=make_filter(args),
            termination_conditions=make_termination_conditions(args),
            aggregation_class=make_aggregation_class(),
            measure_memory=args.measure_memory,
        )

        aggregations = benchmark.run()
        for i, comparator in enumerate(comparators):
            comparator.append(aggregations[i].aggregate())

        c.kill("cockroach", "materialized", "testdrive")
        c.rm("cockroach", "materialized", "testdrive")
        c.rm_volumes("mzdata")

    return comparators


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Feature benchmark framework."""

    c.silent = True

    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )

    parser.add_argument(
        "--measure-memory",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Measure memory usage",
    )

    parser.add_argument(
        "--this-tag",
        metavar="TAG",
        type=str,
        default=os.getenv("THIS_TAG", None),
        help="'This' Materialize container tag to benchmark. If not provided, the current source will be used.",
    )

    parser.add_argument(
        "--this-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("THIS_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'THIS' Mz instance",
    )

    parser.add_argument(
        "--other-tag",
        metavar="TAG",
        type=str,
        default=os.getenv("OTHER_TAG", str(VersionsFromDocs().all_versions()[-1])),
        help="'Other' Materialize container tag to benchmark. If not provided, the last released Mz version will be used.",
    )

    parser.add_argument(
        "--other-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("OTHER_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'OTHER' Mz instance",
    )

    parser.add_argument(
        "--root-scenario",
        "--scenario",
        metavar="SCENARIO",
        type=str,
        default=os.getenv("MZCOMPOSE_SCENARIO", default="Scenario"),
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
        "--max-measurements",
        metavar="N",
        type=int,
        default=99,
        help="Limit the number of measurements to N.",
    )

    parser.add_argument(
        "--max-retries",
        metavar="N",
        type=int,
        default=5,
        help="Retry any potential performance regressions up to N times.",
    )

    parser.add_argument(
        "--this-size",
        metavar="N",
        type=int,
        default=4,
        help="SIZE use for 'THIS'",
    )

    parser.add_argument(
        "--other-size", metavar="N", type=int, default=4, help="SIZE to use for 'OTHER'"
    )

    args = parser.parse_args()

    print(
        dedent(
            f"""
            this_tag: {args.this_tag}
            this_size: {args.this_size}

            other_tag: {args.other_tag}
            other_size: {args.other_size}

            root_scenario: {args.root_scenario}"""
        )
    )

    # Build the list of scenarios to run
    root_scenario = globals()[args.root_scenario]
    initial_scenarios = {}

    if root_scenario.__subclasses__():
        for scenario in root_scenario.__subclasses__():
            has_children = False
            for s in scenario.__subclasses__():
                has_children = True
                initial_scenarios[s] = 1

            if not has_children:
                initial_scenarios[scenario] = 1
    else:
        initial_scenarios[root_scenario] = 1

    dependencies = ["postgres"]

    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    c.up(*dependencies)

    scenarios = initial_scenarios.copy()

    for cycle in range(0, args.max_retries):
        print(
            f"Cycle {cycle+1} with scenarios: {', '.join([scenario.__name__ for scenario in scenarios.keys()])}"
        )

        report = Report()

        for scenario in list(scenarios.keys()):
            comparators = run_one_scenario(c, scenario, args)
            report.extend(comparators)

            # Do not retry the scenario if no regressions
            if all([not c.is_regression() for c in comparators]):
                del scenarios[scenario]

            print(f"+++ Benchmark Report for cycle {cycle+1}:")
            report.dump()

        if len(scenarios.keys()) == 0:
            break

    if len(scenarios.keys()) > 0:
        print(
            f"ERROR: The following scenarios have regressions: {', '.join([scenario.__name__ for scenario in scenarios.keys()])}"
        )
        sys.exit(1)
