# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import sys
from pathlib import Path

import pandas as pd
from jupyter_core.command import main as jupyter_core_command_main
from matplotlib import pyplot as plt

from materialize import buildkite, docker, git
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.scalability.benchmark_config import BenchmarkConfiguration
from materialize.scalability.benchmark_executor import BenchmarkExecutor
from materialize.scalability.benchmark_result import BenchmarkResult
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.endpoints import (
    MaterializeContainer,
    MaterializeLocal,
    MaterializeRemote,
    PostgresContainer,
    endpoint_name_to_description,
)
from materialize.scalability.io import paths
from materialize.scalability.plot.plot import (
    plot_duration_by_connections_for_workload,
    plot_duration_by_endpoints_for_workload,
    plot_tps_per_connections,
)
from materialize.scalability.regression_outcome import RegressionOutcome
from materialize.scalability.result_analyzer import ResultAnalyzer
from materialize.scalability.result_analyzers import DefaultResultAnalyzer
from materialize.scalability.schema import Schema, TransactionIsolation
from materialize.scalability.workload import Workload, WorkloadSelfTest
from materialize.scalability.workloads import *  # noqa: F401 F403
from materialize.scalability.workloads_test import *  # noqa: F401 F403
from materialize.util import all_subclasses

SERVICES = [
    Materialized(
        image="materialize/materialized:latest",
        sanity_restart=False,
        catalog_store="stash",
    ),
    Postgres(),
]

DEFAULT_REGRESSION_THRESHOLD = 0.2

INCLUDE_ZERO_IN_Y_AXIS = True


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--target",
        help="Target for the benchmark: 'HEAD', 'local', 'remote', 'common-ancestor', 'Postgres', or a DockerHub tag",
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
    git.fetch(remote=git.get_remote(), branch="main", include_tags=True)

    schema = Schema(
        create_index=args.create_index,
        transaction_isolation=args.transaction_isolation,
        cluster_name=args.cluster_name,
        object_count=args.object_count,
    )

    result_analyzer = create_result_analyzer(args)

    workload_names = [workload_cls.__name__ for workload_cls in workload_classes]
    df_workloads = pd.DataFrame(data={"workload": workload_names})
    df_workloads.to_csv(paths.workloads_csv())

    config = BenchmarkConfiguration(
        workload_classes=workload_classes,
        exponent_base=args.exponent_base,
        min_concurrency=args.min_concurrency,
        max_concurrency=args.max_concurrency,
        count=args.count,
    )

    executor = BenchmarkExecutor(
        config, schema, baseline_endpoint, other_endpoints, result_analyzer
    )

    benchmark_result = executor.run_workloads()
    result_file_paths = store_results_in_files(benchmark_result)
    upload_results_to_buildkite(result_file_paths)

    create_plots(benchmark_result, baseline_endpoint)
    upload_plots_to_buildkite()

    report_regression_result(
        baseline_endpoint, benchmark_result.overall_regression_outcome
    )


def validate_and_adjust_targets(
    args: argparse.Namespace, regression_against_target: str
) -> None:
    if args.materialize_url is not None and "remote" not in args.target:
        assert False, "--materialize_url requires --target=remote"

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
    baseline_endpoint: Endpoint | None = None
    other_endpoints: list[Endpoint] = []
    for i, target in enumerate(args.target):
        original_target = target
        endpoint: Endpoint | None = None

        if target == "local":
            endpoint = MaterializeLocal()
        elif target == "remote":
            endpoint = MaterializeRemote(materialize_url=args.materialize_url[i])
        elif target == "postgres":
            endpoint = PostgresContainer(composition=c)
        elif target == "HEAD":
            endpoint = MaterializeContainer(
                composition=c, specified_target=original_target
            )
        else:
            if target == "common-ancestor":
                target = docker.resolve_ancestor_image_tag()
            endpoint = MaterializeContainer(
                composition=c,
                specified_target=original_target,
                image=f"materialize/materialized:{target}",
                alternative_image="materialize/materialized:latest",
            )
        assert endpoint is not None

        if original_target == regression_against_target:
            baseline_endpoint = endpoint
        else:
            other_endpoints.append(endpoint)

    return baseline_endpoint, other_endpoints


def get_workload_classes(args: argparse.Namespace) -> list[type[Workload]]:
    return (
        [globals()[workload] for workload in args.workload]
        if args.workload
        else [
            workload_cls
            for workload_cls in all_subclasses(Workload)
            if not issubclass(workload_cls, WorkloadSelfTest)
        ]
    )


def report_regression_result(
    baseline_endpoint: Endpoint | None,
    outcome: RegressionOutcome,
) -> None:
    if baseline_endpoint is None:
        print("No regression detection because '--regression-against' param is not set")
        return

    baseline_desc = endpoint_name_to_description(baseline_endpoint.try_load_version())

    if outcome.has_regressions():
        print(
            f"ERROR: The following regressions were detected (baseline: {baseline_desc}):\n{outcome}"
        )

        if buildkite.is_in_buildkite():
            upload_regressions_to_buildkite(outcome)

        sys.exit(1)
    else:
        print("No regressions were detected.")


def create_result_analyzer(args: argparse.Namespace) -> ResultAnalyzer:
    return DefaultResultAnalyzer(
        max_deviation_as_percent_decimal=args.regression_threshold
    )


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
            baseline_version_name=baseline_endpoint.try_load_version()
            if baseline_endpoint
            else None,
            include_zero_in_y_axis=INCLUDE_ZERO_IN_Y_AXIS,
            include_workload_in_title=True,
        )
        plt.savefig(paths.plot_png("tps", workload_name), bbox_inches="tight", dpi=300)

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


def upload_regressions_to_buildkite(outcome: RegressionOutcome) -> None:
    if not outcome.has_regressions():
        return

    outcome.regression_data.to_csv(paths.regressions_csv())
    buildkite.upload_artifact(
        paths.regressions_csv().relative_to(paths.RESULTS_DIR),
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


def workflow_lab(c: Composition) -> None:
    sys.argv = ["jupyter", "lab", "--no-browser"]
    jupyter_core_command_main()


def workflow_notebook(c: Composition) -> None:
    sys.argv = ["jupyter", "notebook", "--no-browser"]
    jupyter_core_command_main()
