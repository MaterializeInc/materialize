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
from scenarios import *  # noqa: F401 F403
from scenarios import Scenario
from scenarios_concurrency import *  # noqa: F401 F403

from materialize.feature_benchmark.aggregation import Aggregation, MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report, SingleReport
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
    SuccessComparator,
)
from materialize.feature_benchmark.executor import Docker, MzCloud
from materialize.feature_benchmark.filter import Filter, FilterFirst, NoFilter
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
    RunAtMost,
    TerminationCondition,
)
from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import Computed
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


def make_aggregation() -> Aggregation:
    return MinAggregation()


def make_comparator(name: str) -> Comparator:
    return RelativeThresholdComparator(name, threshold=0.10)


default_timeout = "30m"

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    # We are going to override the service definitions during the actual benchmark
    # we put "unstable" here so that we fetch some image from Docker Hub and thus
    # avoid recompiling the current source unless we will actually be benchmarking it.
    Materialized(image="materialize/materialized:unstable"),
    Testdrive(
        validate_data_dir=False,
        default_timeout=default_timeout,
    ),
    KgenService(),
    Postgres(),
]


def start_services(
    c: Composition, args: argparse.Namespace, instance: str
) -> List[Service]:
    tag, options, nodes, workers = (
        (args.this_tag, args.this_options, args.this_nodes, args.this_workers)
        if instance == "this"
        else (args.other_tag, args.other_options, args.other_nodes, args.other_workers)
    )

    cluster_services: List[Service] = []

    if nodes:
        cluster_services.append(
            Materialized(
                image=f"materialize/materialized:{tag}" if tag else None,
            )
        )

        node_names = [f"computed_{n}" for n in range(0, nodes)]
        for node_id in range(0, nodes):
            cluster_services.append(
                Computed(
                    name=node_names[node_id],
                    workers=workers,
                    options=options,
                    peers=node_names,
                    image=f"materialize/computed:{tag}" if tag else None,
                )
            )
    else:
        cluster_services.append(
            Materialized(
                image=f"materialize/materialized:{tag}" if tag else None,
                workers=workers,
                options=options,
            )
        )

    with c.override(*cluster_services):
        print(f"The version of the '{instance.upper()}' Mz instance is:")
        c.run("materialized", "--version")

        # Single-binary legacy Mz instances only have port 6875 open
        # so only check that port before proceeding
        c.up("materialized")
        c.wait_for_materialized(port=6875)

        if nodes:
            print(f"Starting cluster for '{instance.upper()}' ...")
            c.up(*[f"computed_{n}" for n in range(0, nodes)])

            c.sql(
                "CREATE CLUSTER REPLICA default.feature_benchmark REMOTE ["
                + ",".join([f"'computed_{n}:2100'" for n in range(0, nodes)])
                + "];"
            )

            c.sql("DROP CLUSTER REPLICA default.default_replica")

    c.up("testdrive", persistent=True)

    return cluster_services


def stop_services(c: Composition, cluster_services: List[Service]) -> None:
    service_names = [s.name for s in cluster_services] + ["testdrive"]
    c.kill(*service_names)
    c.rm(*service_names)

    c.rm_volumes("mzdata", "pgdata")


def run_one_scenario(
    c: Composition, scenario: Type[Scenario], args: argparse.Namespace
) -> Comparator:
    name = scenario.__name__
    print(f"--- Now benchmarking {name} ...")
    comparator = make_comparator(name)
    common_seed = round(time.time())

    for mz_id, instance in enumerate(["this", "other"]):
        cluster_services = start_services(c, args, instance)

        with c.override(*cluster_services):
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

            stop_services(c, cluster_services)

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
        default=3,
        help="Retry any potential performance regressions up to N times.",
    )

    parser.add_argument(
        "--this-nodes",
        metavar="N",
        type=int,
        default=None,
        help="Start a cluster with that many nodes for 'THIS'",
    )

    parser.add_argument(
        "--other-nodes",
        metavar="N",
        type=int,
        default=None,
        help="Start a cluster with that many nodes for 'OTHER'",
    )

    parser.add_argument(
        "--this-workers",
        metavar="N",
        type=int,
        default=None,
        help="Number of workers to use for 'THIS'",
    )

    parser.add_argument(
        "--other-workers",
        metavar="N",
        type=int,
        default=None,
        help="Number of workers to use for 'OTHER'",
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

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "postgres"]
    )

    scenarios = initial_scenarios.copy()

    for cycle in range(0, args.max_retries):
        print(
            f"Cycle {cycle+1} with scenarios: {', '.join([scenario.__name__ for scenario in scenarios.keys()])}"
        )

        report = Report()

        for scenario in list(scenarios.keys()):
            comparison = run_one_scenario(c, scenario, args)
            report.append(comparison)

            if not comparison.is_regression():
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


def workflow_mzcloud(c: Composition, parser: WorkflowArgumentParser) -> None:
    # Make sure Kafka is externally accessible on a predictable port.
    assert (
        c.preserve_ports
    ), "`--preserve-ports` must be specified (BEFORE the `run` command)"

    parser.add_argument(
        "--mzcloud-url",
        type=str,
        help="The postgres connection url to the mzcloud deployment to benchmark.",
    )

    parser.add_argument(
        "--external-addr",
        type=str,
        help="Kafka and Schema Registry are started by mzcompose and exposed on the public interface. This is the IP address or hostname that is accessible by the mzcloud instance, usually the public IP of your machine.",
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
        default=2,
        help="Retry any potential performance regressions up to N times.",
    )

    parser.add_argument(
        "--test-filter",
        type=str,
        help="Filter scenario names by this string (case insensitive).",
    )

    args = parser.parse_args()

    assert args.mzcloud_url
    assert args.external_addr

    print(
        f"""
mzcloud url: {args.mzcloud_url}
external addr: {args.external_addr}

root_scenario: {args.root_scenario}"""
    )

    overrides = [
        KafkaService(
            extra_environment=[
                f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{args.external_addr}:9092"
            ]
        ),
    ]

    with c.override(*overrides):
        c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
        c.up("testdrive", persistent=True)

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

        scenarios = initial_scenarios.copy()

        for cycle in range(0, args.max_retries):
            print(
                f"Cycle {cycle+1} with scenarios: {', '.join([scenario.__name__ for scenario in scenarios.keys()])}"
            )

            report = SingleReport()

            for scenario in list(scenarios.keys()):
                name = scenario.__name__
                if args.test_filter and args.test_filter.lower() not in name.lower():
                    continue
                print(f"--- Now benchmarking {name} ...")
                comparator = SuccessComparator(name, threshold=0)
                common_seed = round(time.time())
                executor = MzCloud(
                    composition=c,
                    mzcloud_url=args.mzcloud_url,
                    seed=common_seed,
                    external_addr=args.external_addr,
                )
                executor.Reset()
                mz_id = 0

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
                report.append(comparator)

                print(f"+++ Benchmark Report for cycle {cycle+1}:")
                report.dump()
