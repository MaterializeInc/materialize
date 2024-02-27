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

from materialize import buildkite
from materialize.docker import is_image_tag_of_version
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.mysql import MySql
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS,
)
from materialize.version_list import (
    get_commits_of_accepted_regressions_between_versions,
    get_latest_published_version,
    resolve_ancestor_image_tag,
)

# mzcompose may start this script from the root of the Mz repository,
# so we need to explicitly add this directory to the Python module search path
sys.path.append(os.path.dirname(__file__))
from materialize.feature_benchmark.aggregation import Aggregation, MinAggregation
from materialize.feature_benchmark.benchmark import Benchmark, Report
from materialize.feature_benchmark.comparator import (
    Comparator,
    RelativeThresholdComparator,
)
from materialize.feature_benchmark.executor import Docker
from materialize.feature_benchmark.filter import Filter, FilterFirst, NoFilter
from materialize.feature_benchmark.measurement import MeasurementType
from materialize.feature_benchmark.scenarios.benchmark_main import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.benchmark_main import (
    Scenario,
)
from materialize.feature_benchmark.scenarios.concurrency import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.customer import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.optbench import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.scale import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.skew import *  # noqa: F401 F403
from materialize.feature_benchmark.scenarios.subscribe import *  # noqa: F401 F403
from materialize.feature_benchmark.termination import (
    NormalDistributionOverlap,
    ProbForMin,
    RunAtMost,
    TerminationCondition,
)
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka as KafkaService
from materialize.mzcompose.services.kgen import Kgen as KgenService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses

#
# Global feature benchmark thresholds and termination conditions
#


def make_filter(args: argparse.Namespace) -> Filter:
    # Discard the first run unless a small --max-runs limit is explicitly set
    if args.max_measurements <= 5:
        return NoFilter()
    else:
        return FilterFirst()


def make_termination_conditions(args: argparse.Namespace) -> list[TerminationCondition]:
    return [
        NormalDistributionOverlap(threshold=0.95),
        ProbForMin(threshold=0.90),
        RunAtMost(threshold=args.max_measurements),
    ]


def make_aggregation_class() -> type[Aggregation]:
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
    KgenService(),
    Postgres(),
    MySql(),
    Balancerd(),
    # Overridden below
    Materialized(),
    Testdrive(),
]


def run_one_scenario(
    c: Composition, scenario_class: type[Scenario], args: argparse.Namespace
) -> list[Comparator]:
    name = scenario_class.__name__
    print(f"--- Now benchmarking {name} ...")

    measurement_types = [MeasurementType.WALLCLOCK, MeasurementType.MESSAGES]
    if args.measure_memory:
        measurement_types.append(MeasurementType.MEMORY)

    comparators = [make_comparator(name=name, type=t) for t in measurement_types]

    common_seed = round(time.time())

    early_abort = False

    for mz_id, instance in enumerate(["this", "other"]):
        balancerd, tag, size, params = (
            (args.this_balancerd, args.this_tag, args.this_size, args.this_params)
            if instance == "this"
            else (
                args.other_balancerd,
                args.other_tag,
                args.other_size,
                args.other_params,
            )
        )

        if tag == "common-ancestor":
            tag = resolve_ancestor_image_tag(
                ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS
            )

        entrypoint_host = "balancerd" if balancerd else "materialized"

        c.up("testdrive", persistent=True)

        additional_system_parameter_defaults = {"max_clusters": "15"}

        if params is not None:
            for param in params.split(";"):
                param_name, param_value = param.split("=")
                additional_system_parameter_defaults[param_name] = param_value

        mz_image = f"materialize/materialized:{tag}" if tag else None
        mz = create_mz_service(mz_image, size, additional_system_parameter_defaults)

        if tag is not None and not c.try_pull_service_image(mz):
            print(
                f"Unable to find materialize image with tag {tag}, proceeding with latest instead!"
            )
            mz_image = "materialize/materialized:latest"
            mz = create_mz_service(mz_image, size, additional_system_parameter_defaults)

        start_overridden_mz_and_cockroach(c, mz, instance)
        if balancerd:
            c.up("balancerd")

        with c.override(
            Testdrive(
                materialize_url=f"postgres://materialize@{entrypoint_host}:6875",
                default_timeout=default_timeout,
                materialize_params={"statement_timeout": f"'{default_timeout}'"},
            )
        ):
            executor = Docker(composition=c, seed=common_seed, materialized=mz)
            mz_version = MzVersion.parse_mz(c.query_mz_version())

            benchmark = Benchmark(
                mz_id=mz_id,
                mz_version=mz_version,
                scenario=scenario_class,
                scale=args.scale,
                executor=executor,
                filter=make_filter(args),
                termination_conditions=make_termination_conditions(args),
                aggregation_class=make_aggregation_class(),
                measure_memory=args.measure_memory,
                default_size=size,
            )

            if not scenario_class.can_run(mz_version):
                print(
                    f"Skipping scenario {scenario_class} not supported in version {mz_version}"
                )
                early_abort = True
            else:
                aggregations = benchmark.run()
                for aggregation, comparator in zip(aggregations, comparators):
                    comparator.append(aggregation.aggregate())

        c.kill("cockroach", "materialized", "testdrive")
        c.rm("cockroach", "materialized", "testdrive")
        c.rm_volumes("mzdata")

        if early_abort:
            comparators = []
            break

    return comparators


def create_mz_service(
    mz_image: str | None,
    default_size: int,
    additional_system_parameter_defaults: dict[str, str] | None,
) -> Materialized:
    return Materialized(
        image=mz_image,
        default_size=default_size,
        # Avoid clashes with the Kafka sink progress topic across restarts
        environment_id=f"local-az1-{uuid.uuid4()}-0",
        soft_assertions=False,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
        external_cockroach=True,
        external_minio=True,
        sanity_restart=False,
        catalog_store="persist",
    )


def start_overridden_mz_and_cockroach(
    c: Composition, mz: Materialized, instance: str
) -> None:
    with c.override(mz):
        version_request_command = c.run(
            "materialized",
            "-c",
            "environmentd --version | grep environmentd",
            entrypoint="bash",
            rm=True,
            capture=True,
        )
        version = version_request_command.stdout.strip()
        print(f"The version of the '{instance.upper()}' Mz instance is: {version}")

        c.up("cockroach", "materialized")


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
        "--this-balancerd",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use balancerd for THIS",
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
        default=os.getenv("OTHER_TAG", str(get_latest_published_version())),
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
        "--other-balancerd",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use balancerd for OTHER",
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
        default=10,
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
            this_balancerd: {args.this_balancerd}

            other_tag: {args.other_tag}
            other_size: {args.other_size}
            other_balancerd: {args.other_balancerd}

            root_scenario: {args.root_scenario}"""
        )
    )

    # Build the list of scenarios to run
    root_scenario = globals()[args.root_scenario]
    selected_scenarios = []

    if root_scenario.__subclasses__():
        selected_scenarios = sorted(
            [s for s in all_subclasses(root_scenario) if not s.__subclasses__()],
            key=repr,
        )
    else:
        selected_scenarios = [root_scenario]

    dependencies = ["postgres", "mysql"]

    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    c.up(*dependencies)

    scenarios_to_run = [
        scenario
        for i, scenario in enumerate(selected_scenarios)
        if buildkite.accepted_by_shard(i)
    ]

    scenarios_with_regressions = []
    for cycle in range(0, args.max_retries):
        print(
            f"Cycle {cycle+1} with scenarios: {', '.join([scenario.__name__ for scenario in scenarios_to_run])}"
        )

        report = Report()

        scenarios_with_regressions = []
        for scenario in scenarios_to_run:
            comparators = run_one_scenario(c, scenario, args)

            if len(comparators) == 0:
                continue

            report.extend(comparators)

            # Do not retry the scenario if no regressions
            if any([c.is_regression() for c in comparators]):
                scenarios_with_regressions.append(scenario)

            print(f"+++ Benchmark Report for cycle {cycle+1}:")
            report.dump()

        scenarios_to_run = scenarios_with_regressions
        if len(scenarios_to_run) == 0:
            break

    if len(scenarios_with_regressions) > 0:
        regressions_justified, comment = _are_regressions_justified(
            this_tag=args.this_tag, baseline_tag=args.other_tag
        )

        print("+++ Regressions")

        print(
            f"{'INFO' if regressions_justified else 'ERROR'}:"
            f" The following scenarios have regressions:"
            f" {', '.join([scenario.__name__ for scenario in scenarios_with_regressions])}"
        )

        if regressions_justified:
            print(f"However, the regressions are accepted. {comment}")
        else:
            sys.exit(1)


def _are_regressions_justified(
    this_tag: str | None, baseline_tag: str | None
) -> tuple[bool, str]:
    if (
        this_tag is None
        or not _tag_references_release_version(this_tag)
        or not _tag_references_release_version(baseline_tag)
    ):
        return False, ""

    # Checked in _tag_references_release_version
    assert this_tag is not None
    assert baseline_tag is not None

    this_version = MzVersion.parse_mz(this_tag)
    baseline_version = MzVersion.parse_mz(baseline_tag)

    commits_with_regressions = get_commits_of_accepted_regressions_between_versions(
        ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS,
        since_version_exclusive=baseline_version,
        to_version_inclusive=this_version,
    )

    if len(commits_with_regressions) == 0:
        return False, ""
    else:
        return (
            True,
            f"Accepted regressions were introduced with these commits: {commits_with_regressions}",
        )


def _tag_references_release_version(image_tag: str | None) -> bool:
    if image_tag is None:
        return False
    return is_image_tag_of_version(image_tag) and MzVersion.is_valid_version_string(
        image_tag
    )
