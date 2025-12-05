# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Benchmark for how various queries scale, compares against old Materialize versions.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from jupyter_core.command import main as jupyter_core_command_main
from matplotlib import pyplot as plt

from materialize import buildkite, git
from materialize.docker import image_registry
from materialize.mzcompose import ADDITIONAL_BENCHMARKING_SYSTEM_PARAMETERS
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.scalability.config.benchmark_config import BenchmarkConfiguration
from materialize.scalability.df import df_totals_cols
from materialize.scalability.df.df_totals import DfTotalsExtended
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.endpoint.endpoints import (
    TARGET_HEAD,
    TARGET_MATERIALIZE_LOCAL,
    TARGET_MATERIALIZE_REMOTE,
    TARGET_POSTGRES,
    MaterializeContainer,
    MaterializeLocal,
    MaterializeRemote,
    PostgresContainer,
    endpoint_name_to_description,
)
from materialize.scalability.executor.benchmark_executor import BenchmarkExecutor
from materialize.scalability.io import paths
from materialize.scalability.plot.plot import (
    plot_duration_by_connections_for_workload,
    plot_duration_by_endpoints_for_workload,
    plot_tps_per_connections,
)
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.scalability.result.regression_assessment import RegressionAssessment
from materialize.scalability.result.result_analyzer import ResultAnalyzer
from materialize.scalability.result.result_analyzers import DefaultResultAnalyzer
from materialize.scalability.result.scalability_result import BenchmarkResult
from materialize.scalability.schema.schema import Schema, TransactionIsolation
from materialize.scalability.workload.workload import Workload
from materialize.scalability.workload.workload_markers import WorkloadMarker
from materialize.scalability.workload.workloads.connection_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.ddl_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.dml_dql_workloads import *  # noqa: F401 F403
from materialize.scalability.workload.workloads.self_test_workloads import *  # noqa: F401 F403
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.scalability_framework import (
    scalability_framework_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.util import YesNoOnce, all_subclasses
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS,
)
from materialize.version_list import (
    resolve_ancestor_image_tag,
)

SERVICES = [
    Cockroach(setup_materialize=True, in_memory=True),
    Materialized(
        image="{image_registry()}/materialized:latest",
        sanity_restart=False,
        additional_system_parameter_defaults=ADDITIONAL_BENCHMARKING_SYSTEM_PARAMETERS,
        external_metadata_store=True,
        metadata_store="cockroach",
    ),
    Postgres(),
    Balancerd(),
    Mz(app_password=""),
]

DEFAULT_REGRESSION_THRESHOLD = 0.2
SCALABILITY_FRAMEWORK_VERSION = "1.5.0"

INCLUDE_ZERO_IN_Y_AXIS = True


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--target",
        help="Target for the benchmark: 'HEAD', 'local', 'remote', 'common-ancestor', 'postgres', or a DockerHub tag",
        action="append",
        default=[],
    )

    parser.add_argument(
        "--regression-against",
        type=str,
        help="Detect regression against: 'HEAD', 'local', 'remote', 'common-ancestor', 'Postgres', or a DockerHub tag",
        default=None,
    )

    parser.add_argument(
        "--regression-threshold",
        type=float,
        help="Regression threshold (max. relative deterioration from target) as percent decimal",
        default=DEFAULT_REGRESSION_THRESHOLD,
    )

    parser.add_argument(
        "--exponent-base",
        type=float,
        help="Exponent base to use when deciding what concurrencies to test",
        default=2,
    )

    parser.add_argument(
        "--min-concurrency", type=int, help="Minimum concurrency to test", default=1
    )

    parser.add_argument(
        "--max-concurrency",
        type=int,
        help="Maximum concurrency to test",
        default=256,
    )

    parser.add_argument(
        "--workload",
        metavar="WORKLOAD",
        action="append",
        help="Workloads(s) to run.",
    )

    parser.add_argument(
        "--workload-group-marker",
        type=str,
        help="Workload group to run. Required if --workload is not set.",
    )

    parser.add_argument(
        "--count",
        metavar="COUNT",
        type=int,
        default=512,
        help="Number of individual operations to benchmark at concurrency 1 (and COUNT * SQRT(concurrency) for higher concurrencies)",
    )

    parser.add_argument(
        "--object-count",
        metavar="COUNT",
        type=int,
        default=1,
        help="Number of database objects",
    )

    parser.add_argument(
        "--create-index",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Execute a CREATE INDEX",
    )

    parser.add_argument(
        "--transaction-isolation",
        type=TransactionIsolation,
        choices=TransactionIsolation,
        default=None,
        help="SET transaction_isolation",
    )

    parser.add_argument(
        "--materialize-url",
        type=str,
        help="URL to connect to for remote targets",
        action="append",
    )

    parser.add_argument(
        "--use-balancerd",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Whether to communicate through balancerd (only applicable to Materialize containers)",
    )

    parser.add_argument(
        "--verbose",
        default=False,
        type=bool,
        action=argparse.BooleanOptionalAction,
    )

    parser.add_argument("--cluster-name", type=str, help="Cluster to SET CLUSTER to")

    args = parser.parse_args()
    regression_against_target = args.regression_against

    validate_and_adjust_targets(args, regression_against_target)

    baseline_endpoint, other_endpoints = get_baseline_and_other_endpoints(
        c, args, regression_against_target
    )
    workload_classes = get_workload_classes(args)

    print(f"Targets: {args.target}")
    print(f"Checking regression against: {regression_against_target}")
    print("Workloads:")
    for workload_cls in workload_classes:
        print(f"* {workload_cls.__name__}")
    print(f"Baseline: {baseline_endpoint}")
    print("Other endpoints:")
    for other_endpoint in other_endpoints:
        print(f"* {other_endpoint}")

    # fetch main branch and git tags so that their commit messages can be resolved
    git.fetch(remote=git.get_remote(), branch="main", include_tags=YesNoOnce.ONCE)

    schema = Schema(
        create_index=args.create_index,
        transaction_isolation=args.transaction_isolation,
        cluster_name=args.cluster_name,
        object_count=args.object_count,
    )

    regression_threshold = args.regression_threshold
    result_analyzer = create_result_analyzer(regression_threshold)

    workload_names = [workload_cls.__name__ for workload_cls in workload_classes]
    df_workloads = pd.DataFrame(data={"workload": workload_names})
    df_workloads.to_csv(paths.workloads_csv())

    config = BenchmarkConfiguration(
        workload_classes=workload_classes,
        exponent_base=args.exponent_base,
        min_concurrency=args.min_concurrency,
        max_concurrency=args.max_concurrency,
        count=args.count,
        verbose=args.verbose,
    )

    executor = BenchmarkExecutor(
        config, schema, baseline_endpoint, other_endpoints, result_analyzer
    )

    benchmark_result = executor.run_workloads()
    result_file_paths = store_results_in_files(benchmark_result)
    upload_results_to_buildkite(result_file_paths)

    create_plots(benchmark_result, baseline_endpoint)
    upload_plots_to_buildkite()

    regression_assessment = RegressionAssessment(
        baseline_endpoint,
        benchmark_result.overall_comparison_outcome,
    )

    report_regression_result(
        baseline_endpoint,
        regression_threshold,
        benchmark_result.overall_comparison_outcome,
    )

    report_assessment(regression_assessment)

    is_failure = regression_assessment.has_unjustified_regressions()
    upload_results_to_test_analytics(
        c, other_endpoints, benchmark_result, not is_failure
    )

    if is_failure:
        raise FailedTestExecutionError(
            error_summary="At least one regression occurred",
            errors=regression_assessment.to_failure_details(),
        )


def validate_and_adjust_targets(
    args: argparse.Namespace, regression_against_target: str
) -> None:
    if args.materialize_url is not None and "remote" not in args.target:
        raise RuntimeError("--materialize_url requires --target=remote")

    if len(args.target) == 0:
        args.target = ["HEAD"]

    if (
        regression_against_target is not None
        and regression_against_target not in args.target
    ):
        print(f"Adding {regression_against_target} as target")
        args.target.append(regression_against_target)


def get_baseline_and_other_endpoints(
    c: Composition, args: argparse.Namespace, regression_against_target: str
) -> tuple[Endpoint | None, list[Endpoint]]:
    use_balancerd = args.use_balancerd
    baseline_endpoint: Endpoint | None = None
    other_endpoints: list[Endpoint] = []
    for i, specified_target in enumerate(args.target):
        endpoint: Endpoint | None = None

        if specified_target == TARGET_MATERIALIZE_LOCAL:
            endpoint = MaterializeLocal()
        elif specified_target == TARGET_MATERIALIZE_REMOTE:
            endpoint = MaterializeRemote(materialize_url=args.materialize_url[i])
        elif specified_target == TARGET_POSTGRES:
            endpoint = PostgresContainer(composition=c)
        elif specified_target == TARGET_HEAD:
            endpoint = MaterializeContainer(
                composition=c,
                specified_target=specified_target,
                resolved_target=specified_target,
                use_balancerd=use_balancerd,
            )
        else:
            resolved_target = specified_target
            if specified_target == "common-ancestor":
                resolved_target = resolve_ancestor_image_tag(
                    ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS
                )
            endpoint = MaterializeContainer(
                composition=c,
                specified_target=specified_target,
                resolved_target=resolved_target,
                use_balancerd=use_balancerd,
                image=f"{image_registry()}/materialized:{resolved_target}",
                alternative_image="{image_registry()}/materialized:latest",
            )
        assert endpoint is not None

        if specified_target == regression_against_target:
            baseline_endpoint = endpoint
        else:
            other_endpoints.append(endpoint)

    return baseline_endpoint, other_endpoints


def get_workload_classes(args: argparse.Namespace) -> list[type[Workload]]:
    if args.workload:
        workload_classes = [globals()[workload] for workload in args.workload]
    else:
        assert (
            args.workload_group_marker is not None
        ), "--workload-group-marker must be set"

        workload_group_marker_class: type[WorkloadMarker] = globals()[
            args.workload_group_marker
        ]

        workload_classes: list[type[Workload]] = [
            workload_cls for workload_cls in all_subclasses(workload_group_marker_class)
        ]

    assert len(workload_classes) > 0, "No workload class matched"

    # sort classes to ensure a stable order
    workload_classes.sort(key=lambda cls: cls.__name__)

    return workload_classes


def report_regression_result(
    baseline_endpoint: Endpoint | None,
    regression_threshold: float,
    outcome: ComparisonOutcome,
) -> None:
    if baseline_endpoint is None:
        print("No regression detection because '--regression-against' param is not set")
        return

    baseline_desc = endpoint_name_to_description(baseline_endpoint.try_load_version())

    print("+++ Scalability changes")
    if outcome.has_scalability_changes():
        print(
            f"{'ERROR' if outcome.has_regressions() else 'INFO'}: "
            f"The following scalability changes were detected "
            f"(threshold: {regression_threshold}, baseline: {baseline_desc}):\n"
            f"{outcome.to_description()}"
        )

        if buildkite.is_in_buildkite():
            upload_regressions_to_buildkite(outcome)
            upload_significant_improvements_to_buildkite(outcome)

    else:
        print("No scalability changes were detected.")


def report_assessment(regression_assessment: RegressionAssessment):
    print("+++ Assessment of regressions")

    if not regression_assessment.has_comparison_target():
        print("No comparison was performed because not baseline was specified")
        return

    assert regression_assessment.baseline_endpoint is not None

    if not regression_assessment.has_regressions():
        print("No regressions were detected")
        return

    baseline_desc = endpoint_name_to_description(
        regression_assessment.baseline_endpoint.try_load_version()
    )
    for (
        endpoint_with_regression,
        justification,
    ) in regression_assessment.endpoints_with_regressions_and_justifications.items():
        endpoint_desc = endpoint_name_to_description(
            endpoint_with_regression.try_load_version()
        )

        if justification is None:
            print(
                f"* There are regressions between baseline {baseline_desc} and endpoint {endpoint_desc} that need to be checked."
            )
        else:
            print(
                f"* Although there are regressions between baseline {baseline_desc} and endpoint {endpoint_desc},"
                f" they can be explained by the following commits that are marked as accepted regressions: {justification}."
            )


def create_result_analyzer(regression_threshold: float) -> ResultAnalyzer:
    return DefaultResultAnalyzer(max_deviation_as_percent_decimal=regression_threshold)


def create_plots(result: BenchmarkResult, baseline_endpoint: Endpoint | None) -> None:
    paths.plot_dir().mkdir(parents=True, exist_ok=True)

    for (
        workload_name,
        results_by_endpoint,
    ) in result.get_df_total_by_workload_and_endpoint().items():
        fig = plt.figure(layout="constrained", figsize=(16, 6))
        (subfigure) = fig.subfigures(1, 1)
        plot_tps_per_connections(
            workload_name,
            subfigure,
            results_by_endpoint,
            baseline_version_name=(
                baseline_endpoint.try_load_version() if baseline_endpoint else None
            ),
            include_zero_in_y_axis=INCLUDE_ZERO_IN_Y_AXIS,
            include_workload_in_title=True,
        )
        plt.savefig(paths.plot_png("tps", workload_name), bbox_inches="tight", dpi=300)
        plt.close()

    for (
        workload_name,
        results_by_endpoint,
    ) in result.get_df_details_by_workload_and_endpoint().items():
        fig = plt.figure(layout="constrained", figsize=(16, 10))
        (subfigure) = fig.subfigures(1, 1)
        plot_duration_by_connections_for_workload(
            workload_name,
            subfigure,
            results_by_endpoint,
            include_zero_in_y_axis=INCLUDE_ZERO_IN_Y_AXIS,
            include_workload_in_title=True,
        )
        plt.savefig(
            paths.plot_png("duration_by_connections", workload_name),
            bbox_inches="tight",
            dpi=300,
        )
        plt.close()

        fig = plt.figure(layout="constrained", figsize=(16, 10))
        (subfigure) = fig.subfigures(1, 1)
        plot_duration_by_endpoints_for_workload(
            workload_name,
            subfigure,
            results_by_endpoint,
            include_zero_in_y_axis=INCLUDE_ZERO_IN_Y_AXIS,
            include_workload_in_title=True,
        )
        plt.savefig(
            paths.plot_png("duration_by_endpoints", workload_name),
            bbox_inches="tight",
            dpi=300,
        )
        plt.close()


def upload_regressions_to_buildkite(outcome: ComparisonOutcome) -> None:
    if outcome.has_regressions():
        _upload_scalability_changes_to_buildkite(
            outcome.regression_df, paths.regressions_csv()
        )


def upload_significant_improvements_to_buildkite(outcome: ComparisonOutcome) -> None:
    if outcome.has_significant_improvements():
        _upload_scalability_changes_to_buildkite(
            outcome.significant_improvement_df, paths.significant_improvements_csv()
        )


def _upload_scalability_changes_to_buildkite(
    scalability_changes: DfTotalsExtended, file_path: Path
) -> None:
    scalability_changes.to_csv(file_path)
    buildkite.upload_artifact(
        file_path.relative_to(paths.RESULTS_DIR),
        cwd=paths.RESULTS_DIR,
    )


def store_results_in_files(result: BenchmarkResult) -> list[Path]:
    created_files = []
    for endpoint_name in result.get_endpoint_names():
        print(
            f"Writing results of {endpoint_name} to {paths.results_csv(endpoint_name)}"
        )
        df_total = result.get_df_total_by_endpoint_name(endpoint_name)
        file_path = paths.results_csv(endpoint_name)
        df_total.to_csv(file_path)
        created_files.append(file_path)

    return created_files


def upload_results_to_buildkite(result_file_paths: list[Path]) -> None:
    if not buildkite.is_in_buildkite():
        return

    for path in result_file_paths:
        buildkite.upload_artifact(
            path.relative_to(paths.RESULTS_DIR),
            cwd=paths.RESULTS_DIR,
        )


def upload_plots_to_buildkite() -> None:
    if not buildkite.is_in_buildkite():
        return

    buildkite.upload_artifact(
        f"{paths.plot_dir().relative_to(paths.RESULTS_DIR)}/*.png",
        cwd=paths.RESULTS_DIR,
    )


def upload_results_to_test_analytics(
    c: Composition,
    endpoints: list[Endpoint],
    benchmark_result: BenchmarkResult,
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    head_target_endpoint = _get_head_target_endpoint(endpoints)

    if head_target_endpoint is None:
        print("Not uploading results because not HEAD version included in endpoints")
        return

    endpoint_version_info = head_target_endpoint.try_load_version()
    results_of_endpoint = benchmark_result.df_total_by_endpoint_name_and_workload[
        endpoint_version_info
    ]

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    for workload_name, result in results_of_endpoint.items():
        workload_version = benchmark_result.workload_version_by_name[workload_name]
        workload_group = benchmark_result.workload_group_by_name[workload_name]

        for index, row in result.data.iterrows():
            result_entries.append(
                scalability_framework_result_storage.ScalabilityFrameworkResultEntry(
                    workload_name=workload_name,
                    workload_group=workload_group,
                    workload_version=str(workload_version),
                    concurrency=row[df_totals_cols.CONCURRENCY],
                    count=row[df_totals_cols.COUNT],
                    tps=row[df_totals_cols.TPS],
                )
            )

    test_analytics.scalability_results.add_result(
        framework_version=SCALABILITY_FRAMEWORK_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def _get_head_target_endpoint(endpoints: list[Endpoint]) -> Endpoint | None:
    for endpoint in endpoints:
        if endpoint.specified_target() == TARGET_HEAD:
            return endpoint

    return None


def workflow_lab(c: Composition) -> None:
    sys.argv = ["jupyter", "lab", "--no-browser"]
    jupyter_core_command_main()


def workflow_notebook(c: Composition) -> None:
    sys.argv = ["jupyter", "notebook", "--no-browser"]
    jupyter_core_command_main()
