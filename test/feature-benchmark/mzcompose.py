# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Simple benchmark of mostly individual queries using testdrive. Can find
wallclock/memorys regressions in single-connection query executions, not
suitable for concurrency.
"""

import argparse
import os
import sys
import time
import uuid
from textwrap import dedent

from materialize import buildkite
from materialize.docker import is_image_tag_of_release_version
from materialize.feature_benchmark.benchmark_result_evaluator import (
    BenchmarkResultEvaluator,
)
from materialize.feature_benchmark.benchmark_result_selection import (
    BestBenchmarkResultSelector,
    get_discarded_reports_per_scenario,
)
from materialize.feature_benchmark.report import (
    Report,
    determine_scenario_classes_with_regressions,
)
from materialize.mz_version import MzVersion
from materialize.mzcompose import ADDITIONAL_BENCHMARKING_SYSTEM_PARAMETERS
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.feature_benchmark import (
    feature_benchmark_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.version_ancestor_overrides import (
    get_ancestor_overrides_for_performance_regressions,
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
from materialize.feature_benchmark.benchmark import Benchmark
from materialize.feature_benchmark.benchmark_result import (
    BenchmarkScenarioResult,
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
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka as KafkaService
from materialize.mzcompose.services.kgen import Kgen as KgenService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses

#
# Global feature benchmark thresholds and termination conditions
#


FEATURE_BENCHMARK_FRAMEWORK_VERSION = "1.5.0"


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


default_timeout = "1800s"

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    Redpanda(),
    Cockroach(setup_materialize=True, in_memory=True),
    Minio(setup_materialize=True),
    Azurite(),
    KgenService(),
    Postgres(),
    MySql(),
    Balancerd(),
    # Overridden below
    Materialized(),
    Clusterd(),
    Testdrive(),
    Mz(app_password=""),
]


def run_one_scenario(
    c: Composition,
    scenario_class: type[Scenario],
    args: argparse.Namespace,
    dependencies: list[str],
) -> BenchmarkScenarioResult:
    scenario_name = scenario_class.__name__
    print(f"--- Now benchmarking {scenario_name} ...")

    measurement_types = [MeasurementType.WALLCLOCK]
    if args.measure_memory:
        measurement_types.append(MeasurementType.MEMORY_MZ)
        measurement_types.append(MeasurementType.MEMORY_CLUSTERD)

    result = BenchmarkScenarioResult(scenario_class, measurement_types)

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

        tag = resolve_tag(tag, scenario_class, args.scale)

        entrypoint_host = "balancerd" if balancerd else "materialized"

        c.up(Service("testdrive", idle=True))

        additional_system_parameter_defaults = (
            ADDITIONAL_BENCHMARKING_SYSTEM_PARAMETERS
            | {
                "max_clusters": "15",
                "enable_unorchestrated_cluster_replicas": "true",
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
            }
        )

        if params is not None:
            for param in params.split(";"):
                param_name, param_value = param.split("=")
                additional_system_parameter_defaults[param_name] = param_value

        mz_image = f"materialize/materialized:{tag}" if tag else None
        # TODO: Better azurite support detection
        mz = create_mz_service(
            mz_image,
            size,
            additional_system_parameter_defaults,
            args.azurite and instance == "this",
        )
        clusterd_image = f"materialize/clusterd:{tag}" if tag else None
        clusterd = create_clusterd_service(
            clusterd_image, size, additional_system_parameter_defaults
        )

        if tag is not None and not c.try_pull_service_image(mz):
            print(
                f"Unable to find materialize image with tag {tag}, proceeding with latest instead!"
            )
            mz_image = "materialize/materialized:latest"
            # TODO: Better azurite support detection
            mz = create_mz_service(
                mz_image,
                size,
                additional_system_parameter_defaults,
                args.azurite and instance == "this",
            )
            clusterd_image = f"materialize/clusterd:{tag}" if tag else None
            clusterd = create_clusterd_service(
                clusterd_image, size, additional_system_parameter_defaults
            )

        start_overridden_mz_clusterd_and_cockroach(c, mz, clusterd, instance, balancerd)

        with c.override(
            Testdrive(
                materialize_url=f"postgres://materialize@{entrypoint_host}:6875",
                default_timeout=default_timeout,
                materialize_params={"statement_timeout": f"'{default_timeout}'"},
                metadata_store="cockroach",
                external_blob_store=True,
                blob_store_is_azure=args.azurite,
            )
        ):
            c.testdrive(
                dedent(
                    """
                    $[version<9000] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;

                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    CREATE CLUSTER cluster_default REPLICAS (r1 (STORAGECTL ADDRESSES ['clusterd:2100'], STORAGE ADDRESSES ['clusterd:2103'], COMPUTECTL ADDRESSES ['clusterd:2101'], COMPUTE ADDRESSES ['clusterd:2102'], WORKERS 1));
                    ALTER SYSTEM SET cluster = cluster_default;
                    GRANT ALL PRIVILEGES ON CLUSTER cluster_default TO materialize;"""
                ),
            )

            executor = Docker(
                composition=c, seed=common_seed, materialized=mz, clusterd=clusterd
            )
            mz_version = MzVersion.parse_mz(c.query_mz_version())

            benchmark = Benchmark(
                mz_id=mz_id,
                mz_version=mz_version,
                scenario_cls=scenario_class,
                scale=args.scale,
                executor=executor,
                filter=make_filter(args),
                termination_conditions=make_termination_conditions(args),
                aggregation_class=make_aggregation_class(),
                measure_memory=args.measure_memory,
                default_size=size,
                seed=common_seed,
            )

            if not scenario_class.can_run(mz_version):
                print(
                    f"Skipping scenario {scenario_class} not supported in version {mz_version}"
                )
                early_abort = True
            else:
                aggregations = benchmark.run()
                scenario_version = benchmark.create_scenario_instance().version()
                result.set_scenario_version(scenario_version)
                for aggregation, metric in zip(aggregations, result.metrics):
                    assert (
                        aggregation.measurement_type == metric.measurement_type
                        or aggregation.measurement_type is None
                    ), f"Aggregation contains {aggregation.measurement_type} but metric contains {metric.measurement_type} as measurement type"
                    metric.append_point(
                        aggregation.aggregate(),
                        aggregation.unit(),
                        aggregation.name(),
                    )

        c.kill("cockroach", "materialized", "clusterd", "testdrive", *dependencies)
        c.rm("cockroach", "materialized", "clusterd", "testdrive", *dependencies)
        c.rm_volumes("mzdata")
        c.up(*dependencies)

        if early_abort:
            result.empty()
            break

    return result


resolved_tags: dict[tuple[str, frozenset[tuple[str, MzVersion]]], str] = {}


def resolve_tag(tag: str, scenario_class: type[Scenario], scale: str | None) -> str:
    if tag == "common-ancestor":
        overrides = get_ancestor_overrides_for_performance_regressions(
            scenario_class, scale
        )
        key = (tag, frozenset(overrides.items()))
        if key not in resolved_tags:
            resolved_tags[key] = resolve_ancestor_image_tag(overrides)
        return resolved_tags[key]

    return tag


def create_mz_service(
    mz_image: str | None,
    default_size: int,
    additional_system_parameter_defaults: dict[str, str] | None,
    azurite: bool,
) -> Materialized:
    return Materialized(
        image=mz_image,
        default_size=default_size,
        # Avoid clashes with the Kafka sink progress topic across restarts
        environment_id=f"local-az1-{uuid.uuid4()}-0",
        soft_assertions=False,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
        external_metadata_store=True,
        metadata_store="cockroach",
        external_blob_store=True,
        blob_store_is_azure=azurite,
        sanity_restart=False,
        support_external_clusterd=True,
    )


def create_clusterd_service(
    clusterd_image: str | None,
    default_size: int,
    additional_system_parameter_defaults: dict[str, str] | None,
) -> Clusterd:
    return Clusterd(image=clusterd_image)


def start_overridden_mz_clusterd_and_cockroach(
    c: Composition, mz: Materialized, clusterd: Clusterd, instance: str, balancerd: bool
) -> None:
    with c.override(mz, clusterd):
        c.up(
            "cockroach",
            "materialized",
            "clusterd",
            *(["balancerd"] if balancerd else []),
        )

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
        c.verify_build_profile()


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
        "--runs-per-scenario",
        metavar="N",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--this-size",
        metavar="N",
        type=int,
        default=4,
        help="SIZE use for 'THIS'",
    )

    parser.add_argument(
        "--ignore-other-tag-missing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Don't run anything if 'OTHER' tag is missing",
    )

    parser.add_argument(
        "--other-size", metavar="N", type=int, default=4, help="SIZE to use for 'OTHER'"
    )
    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
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

    specified_root_scenario = globals()[args.root_scenario]

    if specified_root_scenario.__subclasses__():
        selected_scenarios = sorted(
            # collect all leafs of the specified root scenario
            [
                s
                for s in all_subclasses(specified_root_scenario)
                if not s.__subclasses__()
            ],
            key=repr,
        )
    else:
        # specified root scenario is a leaf
        selected_scenarios = [specified_root_scenario]

    dependencies = ["postgres", "mysql"]

    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    c.up(*dependencies)

    scenario_classes_scheduled_to_run: list[type[Scenario]] = buildkite.shard_list(
        selected_scenarios, lambda scenario_cls: scenario_cls.__name__
    )

    if (
        len(scenario_classes_scheduled_to_run) == 0
        and buildkite.is_in_buildkite()
        and not os.getenv("CI_EXTRA_ARGS")
    ):
        raise FailedTestExecutionError(
            error_summary="No scenarios were selected", errors=[]
        )

    reports = []

    for run_index in range(0, args.runs_per_scenario):
        run_number = run_index + 1
        print(
            f"Run {run_number} with scenarios: {', '.join([scenario.__name__ for scenario in scenario_classes_scheduled_to_run])}"
        )

        report = Report(cycle_number=run_number)
        reports.append(report)

        for scenario_class in scenario_classes_scheduled_to_run:
            try:
                scenario_result = run_one_scenario(
                    c, scenario_class, args, dependencies
                )
            except RuntimeError as e:
                if (
                    "No image found for commit hash" in str(e)
                    and args.ignore_other_tag_missing
                ):
                    print(
                        "Missing image for base, which can happen when main branch fails to build, ignoring"
                    )
                    return
                raise e

            if scenario_result.is_empty():
                continue

            report.add_scenario_result(scenario_result)

        print(f"+++ Benchmark Report for run {run_number}:")
        print(report)

    benchmark_result_selector = BestBenchmarkResultSelector()
    selected_report_by_scenario_name = (
        benchmark_result_selector.choose_report_per_scenario(reports)
    )
    discarded_reports_by_scenario_name = get_discarded_reports_per_scenario(
        reports, selected_report_by_scenario_name
    )

    scenarios_with_regressions = determine_scenario_classes_with_regressions(
        selected_report_by_scenario_name
    )

    if len(scenarios_with_regressions) > 0:
        justification_by_scenario_name = _check_regressions_justified(
            scenarios_with_regressions,
            this_tag=args.this_tag,
            baseline_tag=args.other_tag,
            scale=args.scale,
        )

        justifications = [
            justification
            for justification in justification_by_scenario_name.values()
            if justification is not None
        ]

        all_regressions_justified = len(justification_by_scenario_name) == len(
            justifications
        )

        print("+++ Regressions")

        print(
            f"{'INFO' if all_regressions_justified else 'ERROR'}:"
            f" The following scenarios have regressions:"
            f" {', '.join([scenario.__name__ for scenario in scenarios_with_regressions])}"
        )

        if all_regressions_justified:
            print("All regressions are justified:")
            print("\n".join(justifications))
            successful_run = True
        elif len(justifications) > 0:
            print("Some regressions are justified:")
            print("\n".join(justifications))
            successful_run = False
        else:
            successful_run = False
    else:
        successful_run = True
        justification_by_scenario_name = dict()

    upload_results_to_test_analytics(
        c,
        args.this_tag,
        scenario_classes_scheduled_to_run,
        scenarios_with_regressions,
        args.scale,
        selected_report_by_scenario_name,
        discarded_reports_by_scenario_name,
        successful_run,
    )

    if not successful_run:
        raise FailedTestExecutionError(
            error_summary="At least one regression occurred",
            errors=_regressions_to_failure_details(
                scenarios_with_regressions,
                selected_report_by_scenario_name,
                justification_by_scenario_name,
                baseline_tag=args.other_tag,
                scale=args.scale,
            ),
        )


def is_regression(
    evaluator: BenchmarkResultEvaluator, scenario_result: BenchmarkScenarioResult
) -> bool:
    return any([evaluator.is_regression(metric) for metric in scenario_result.metrics])


def _check_regressions_justified(
    scenarios_with_regressions: list[type[Scenario]],
    this_tag: str | None,
    baseline_tag: str | None,
    scale: str | None,
) -> dict[str, str | None]:
    """
    :return: justification per scenario name if justified else None
    """
    justification_by_scenario_name: dict[str, str | None] = dict()

    for scenario_class in scenarios_with_regressions:
        regressions_justified, comment = _is_regression_justified(
            scenario_class, this_tag=this_tag, baseline_tag=baseline_tag, scale=scale
        )

        justification_by_scenario_name[scenario_class.__name__] = (
            comment if regressions_justified else None
        )

    return justification_by_scenario_name


def _is_regression_justified(
    scenario_class: type[Scenario],
    this_tag: str | None,
    baseline_tag: str | None,
    scale: str | None,
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
        get_ancestor_overrides_for_performance_regressions(scenario_class, scale),
        since_version_exclusive=baseline_version,
        to_version_inclusive=this_version,
    )

    if len(commits_with_regressions) == 0:
        return False, ""

    return (
        True,
        f"* {scenario_class.__name__}: Justified regressions were introduced with commits {commits_with_regressions}.",
    )


def _tag_references_release_version(image_tag: str | None) -> bool:
    if image_tag is None:
        return False
    return is_image_tag_of_release_version(
        image_tag
    ) and MzVersion.is_valid_version_string(image_tag)


def _regressions_to_failure_details(
    scenarios_with_regressions: list[type[Scenario]],
    latest_report_by_scenario_name: dict[str, Report],
    justification_by_scenario_name: dict[str, str | None],
    baseline_tag: str,
    scale: str | None,
) -> list[TestFailureDetails]:
    failure_details = []

    for scenario_cls in scenarios_with_regressions:
        scenario_name = scenario_cls.__name__

        if justification_by_scenario_name[scenario_name] is not None:
            continue

        regression_against_tag = resolve_tag(baseline_tag, scenario_cls, scale)

        report = latest_report_by_scenario_name[scenario_name]
        failure_details.append(
            TestFailureDetails(
                test_case_name_override=f"Scenario '{scenario_name}'",
                message=f"New regression against {regression_against_tag}",
                details=report.as_string(
                    use_colors=False, limit_to_scenario=scenario_name
                ),
            )
        )

    return failure_details


def upload_results_to_test_analytics(
    c: Composition,
    this_tag: str | None,
    scenario_classes: list[type[Scenario]],
    scenarios_with_regressions: list[type[Scenario]],
    scale: str | None,
    latest_report_by_scenario_name: dict[str, Report],
    discarded_reports_by_scenario_name: dict[str, list[Report]],
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    if this_tag is not None:
        # only include measurements on HEAD
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []
    discarded_entries = []

    for scenario_cls in scenario_classes:
        scenario_name = scenario_cls.__name__
        report = latest_report_by_scenario_name[scenario_name]

        result_entries.append(
            _create_feature_benchmark_result_entry(
                scenario_cls,
                report,
                scale,
                is_regression=scenario_cls in scenarios_with_regressions,
            )
        )

        for discarded_report in discarded_reports_by_scenario_name.get(
            scenario_name, []
        ):
            discarded_entries.append(
                _create_feature_benchmark_result_entry(
                    scenario_cls, discarded_report, scale, is_regression=True
                )
            )

    test_analytics.benchmark_results.add_result(
        framework_version=FEATURE_BENCHMARK_FRAMEWORK_VERSION,
        results=result_entries,
    )
    test_analytics.benchmark_results.add_discarded_entries(discarded_entries)

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def _create_feature_benchmark_result_entry(
    scenario_cls: type[Scenario], report: Report, scale: str | None, is_regression: bool
) -> feature_benchmark_result_storage.FeatureBenchmarkResultEntry:
    scenario_name = scenario_cls.__name__
    scenario_group = scenario_cls.__bases__[0].__name__
    scenario_version = report.get_scenario_version(scenario_name)
    measurements = report.measurements_of_this(scenario_name)

    return feature_benchmark_result_storage.FeatureBenchmarkResultEntry(
        scenario_name=scenario_name,
        scenario_group=scenario_group,
        scenario_version=str(scenario_version),
        cycle=report.cycle_number,
        scale=scale or "default",
        is_regression=is_regression,
        wallclock=measurements[MeasurementType.WALLCLOCK],
        memory_mz=measurements[MeasurementType.MEMORY_MZ],
        memory_clusterd=measurements[MeasurementType.MEMORY_CLUSTERD],
    )
