# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import pathlib
import sys
import threading
import time
from concurrent import futures
from math import floor, sqrt
from typing import Any

import pandas as pd
from jupyter_core.command import main as jupyter_core_command_main
from psycopg import Cursor

from materialize import benchmark_utils, buildkite, spawn
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.scalability.df import df_details_cols, df_totals_cols, paths
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.endpoints import (
    MaterializeContainer,
    MaterializeLocal,
    MaterializeRemote,
    PostgresContainer,
    endpoint_name_to_description,
)
from materialize.scalability.operation import Operation
from materialize.scalability.regression import RegressionOutcome
from materialize.scalability.result_analyzer import ResultAnalyzer
from materialize.scalability.result_analyzers import DefaultResultAnalyzer
from materialize.scalability.schema import Schema, TransactionIsolation
from materialize.scalability.workload import Workload, WorkloadSelfTest
from materialize.scalability.workload_result import WorkloadResult
from materialize.scalability.workloads import *  # noqa: F401 F403
from materialize.scalability.workloads_test import *  # noqa: F401 F403
from materialize.util import all_subclasses

SERVICES = [
    Materialized(image="materialize/materialized:latest", sanity_restart=False),
    Postgres(),
]


def initialize_worker(local: threading.local, lock: threading.Lock):
    """Give each other worker thread a unique ID"""
    lock.acquire()
    global next_worker_id
    local.worker_id = next_worker_id
    next_worker_id = next_worker_id + 1
    lock.release()


def execute_operation(
    args: tuple[Workload, int, threading.local, list[Cursor], Operation]
) -> dict[str, Any]:
    workload, concurrency, local, cursor_pool, operation = args
    assert (
        len(cursor_pool) >= local.worker_id + 1
    ), f"len(cursor_pool) is {len(cursor_pool)} but local.worker_id is {local.worker_id}"
    cursor = cursor_pool[local.worker_id]

    start = time.time()
    operation.execute(cursor)
    wallclock = time.time() - start

    return {
        df_details_cols.CONCURRENCY: concurrency,
        df_details_cols.WALLCLOCK: wallclock,
        df_details_cols.OPERATION: type(operation).__name__,
        df_details_cols.WORKLOAD: type(workload).__name__,
    }


def run_with_concurrency(
    c: Composition,
    endpoint: Endpoint,
    schema: Schema,
    workload: Workload,
    concurrency: int,
    count: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    print(
        f"Preparing benchmark for workload '{workload.name()}' at concurrency {concurrency} ..."
    )
    endpoint.up()

    init_sqls = schema.init_sqls()

    init_conn = endpoint.sql_connection()
    init_conn.autocommit = True
    init_cursor = init_conn.cursor()
    for init_sql in init_sqls:
        print(init_sql)
        init_cursor.execute(init_sql.encode("utf8"))

    connect_sqls = schema.connect_sqls()

    print(
        f"Creating a cursor pool with {concurrency} entries against endpoint: {endpoint.url()}"
    )
    cursor_pool = []
    for i in range(concurrency):
        conn = endpoint.sql_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        for connect_sql in connect_sqls:
            cursor.execute(connect_sql.encode("utf8"))
        cursor_pool.append(cursor)

    print(f"Benchmarking workload '{workload.name()}' at concurrency {concurrency} ...")
    operations = workload.operations()

    global next_worker_id
    next_worker_id = 0
    local = threading.local()
    lock = threading.Lock()

    start = time.time()
    with futures.ThreadPoolExecutor(
        concurrency, initializer=initialize_worker, initargs=(local, lock)
    ) as executor:
        measurements = executor.map(
            execute_operation,
            [
                (
                    workload,
                    concurrency,
                    local,
                    cursor_pool,
                    operations[i % len(operations)],
                )
                for i in range(count)
            ],
        )
    wallclock_total = time.time() - start

    df_detail = pd.DataFrame(measurements)
    print("Best and worst individual measurements:")
    print(df_detail.sort_values(by=[df_details_cols.WALLCLOCK]))

    print(
        f"concurrency: {concurrency}; wallclock_total: {wallclock_total}; tps = {count/wallclock_total}"
    )

    df_total = pd.DataFrame(
        [
            {
                df_totals_cols.CONCURRENCY: concurrency,
                df_totals_cols.WALLCLOCK: wallclock_total,
                df_totals_cols.WORKLOAD: type(workload).__name__,
                df_totals_cols.COUNT: count,
                df_totals_cols.TPS: count / wallclock_total,
                "mean_t_dur": df_detail["wallclock"].mean(),
                "median_t_dur": df_detail["wallclock"].median(),
                "min_t_dur": df_detail["wallclock"].min(),
                "max_t_dur": df_detail["wallclock"].max(),
            }
        ]
    )

    return (df_total, df_detail)


def run_workload(
    c: Composition,
    args: argparse.Namespace,
    endpoint: Endpoint,
    schema: Schema,
    workload: Workload,
) -> WorkloadResult:
    df_totals = pd.DataFrame()
    df_details = pd.DataFrame()

    concurrencies: list[int] = [round(args.exponent_base**c) for c in range(0, 1024)]
    concurrencies = sorted(set(concurrencies))
    concurrencies = [
        c
        for c in concurrencies
        if c >= args.min_concurrency and c <= args.max_concurrency
    ]
    print(f"Concurrencies: {concurrencies}")

    for concurrency in concurrencies:
        df_total, df_detail = run_with_concurrency(
            c,
            endpoint,
            schema,
            workload,
            concurrency,
            floor(args.count * sqrt(concurrency)),
        )
        df_totals = pd.concat([df_totals, df_total], ignore_index=True)
        df_details = pd.concat([df_details, df_detail], ignore_index=True)

        endpoint_name = endpoint.name()
        pathlib.Path(paths.endpoint_dir(endpoint_name)).mkdir(
            parents=True, exist_ok=True
        )

        df_totals.to_csv(paths.df_totals_csv(endpoint_name, workload))
        df_details.to_csv(paths.df_details_csv(endpoint_name, workload))

    return WorkloadResult(workload, df_totals, df_details)


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

    if args.materialize_url is not None and "remote" not in args.target:
        assert False, "--materialize_url requires --target=remote"

    if len(args.target) == 0:
        args.target = ["HEAD"]

    regression_against_target = args.regression_against
    if (
        regression_against_target is not None
        and regression_against_target not in args.target
    ):
        print(f"Adding {regression_against_target} as target")
        args.target.append(regression_against_target)

    print(f"Targets: {args.target}")
    print(f"Checking regression against: {regression_against_target}")

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
            endpoint = MaterializeContainer(composition=c)
        else:
            if target == "common-ancestor":
                target = benchmark_utils.resolve_tag_of_common_ancestor()
            endpoint = MaterializeContainer(
                composition=c,
                image=f"materialize/materialized:{target}",
                alternative_image="materialize/materialized:latest",
            )
        assert endpoint is not None

        if original_target == regression_against_target:
            baseline_endpoint = endpoint
        else:
            other_endpoints.append(endpoint)

    workloads = (
        [globals()[workload] for workload in args.workload]
        if args.workload
        else [
            workload_cls
            for workload_cls in all_subclasses(Workload)
            if not issubclass(workload_cls, WorkloadSelfTest)
        ]
    )

    schema = Schema(
        create_index=args.create_index,
        transaction_isolation=args.transaction_isolation,
        cluster_name=args.cluster_name,
        object_count=args.object_count,
    )

    result_analyzer = create_result_analyzer(args)

    workload_names = [workload.__name__ for workload in workloads]
    df_workloads = pd.DataFrame(data={"workload": workload_names})
    df_workloads.to_csv(paths.workloads_csv())

    overall_regression_outcome = RegressionOutcome()

    for workload in workloads:
        assert issubclass(workload, Workload), f"{workload} is not a Workload"
        workload_name = workload.__name__

        if baseline_endpoint is not None:
            baseline_result = run_workload(
                c, args, baseline_endpoint, schema, workload()
            )
        else:
            baseline_result = None

        for other_endpoint in other_endpoints:
            other_endpoint_result = run_workload(
                c, args, other_endpoint, schema, workload()
            )

            if baseline_endpoint is not None and baseline_result is not None:
                regression_outcome = result_analyzer.determine_regression_in_workload(
                    workload_name,
                    baseline_endpoint,
                    other_endpoint,
                    baseline_result,
                    other_endpoint_result,
                )
                overall_regression_outcome.merge(regression_outcome)

    report_regression_result(baseline_endpoint, overall_regression_outcome)


def report_regression_result(
    baseline_endpoint: Endpoint | None,
    outcome: RegressionOutcome,
) -> None:
    if baseline_endpoint is None:
        print("No regression detection because '--regression-against' param is not set")
        return

    baseline_desc = endpoint_name_to_description(baseline_endpoint.name())

    if outcome.has_regressions():
        print(
            f"ERROR: The following regressions were detected (baseline: {baseline_desc}):\n{outcome}"
        )

        if buildkite.is_in_buildkite():
            upload_regressions_to_buildkite(outcome)

        sys.exit(1)
    else:
        print("No regressions were detected.")


def create_result_analyzer(_args: argparse.Namespace) -> ResultAnalyzer:
    return DefaultResultAnalyzer(max_deviation_in_percent=0.1)


def upload_regressions_to_buildkite(outcome: RegressionOutcome) -> None:
    if not outcome.has_regressions():
        return

    outcome.raw_regression_data.to_csv(paths.regressions_csv())
    spawn.runv(
        ["buildkite-agent", "artifact", "upload", paths.regressions_csv_name()],
        cwd=paths.RESULTS_DIR,
    )


def workflow_lab(c: Composition) -> None:
    sys.argv = ["jupyter", "lab", "--no-browser"]
    jupyter_core_command_main()


def workflow_notebook(c: Composition) -> None:
    sys.argv = ["jupyter", "notebook", "--no-browser"]
    jupyter_core_command_main()
