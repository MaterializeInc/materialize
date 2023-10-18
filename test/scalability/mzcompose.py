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

from materialize import MZ_ROOT, benchmark_utils
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.endpoints import (
    MaterializeContainer,
    MaterializeLocal,
    MaterializeRemote,
    PostgresContainer,
)
from materialize.scalability.operation import Operation
from materialize.scalability.schema import Schema, TransactionIsolation
from materialize.scalability.workload import Workload, WorkloadSelfTest
from materialize.scalability.workloads import *  # noqa: F401 F403
from materialize.scalability.workloads_test import *  # noqa: F401 F403

RESULTS_DIR = MZ_ROOT / "test" / "scalability" / "results"
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
        "concurrency": concurrency,
        "wallclock": wallclock,
        "operation": type(operation).__name__,
        "workload": type(workload).__name__,
    }


def run_with_concurrency(
    c: Composition,
    endpoint: Endpoint,
    schema: Schema,
    workload: Workload,
    concurrency: int,
    count: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    print(f"Benchmarking workload {type(workload)} at concurrency {concurrency} ...")
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
    print(df_detail.sort_values(by=["wallclock"]))

    print(
        f"concurrency: {concurrency}; wallclock_total: {wallclock_total}; tps = {count/wallclock_total}"
    )

    df_total = pd.DataFrame(
        [
            {
                "concurrency": concurrency,
                "wallclock": wallclock_total,
                "workload": type(workload).__name__,
                "count": count,
                "tps": count / wallclock_total,
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
) -> None:
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
        pathlib.Path(RESULTS_DIR / endpoint_name).mkdir(parents=True, exist_ok=True)

        df_totals.to_csv(RESULTS_DIR / endpoint_name / f"{type(workload).__name__}.csv")
        df_details.to_csv(
            RESULTS_DIR / endpoint_name / f"{type(workload).__name__}_details.csv"
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--target",
        help="Target for the benchmark: 'HEAD', 'local', 'remote', 'common-ancestor', 'Postgres', or a DockerHub tag",
        action="append",
        default=[],
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

    print(f"Targets: {args.target}")

    endpoints: list[Endpoint] = []
    for i, target in enumerate(args.target):
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

        endpoints.append(endpoint)

    workloads = (
        [globals()[workload] for workload in args.workload]
        if args.workload
        else [w for w in Workload.__subclasses__() if not w == WorkloadSelfTest]
    )

    schema = Schema(
        create_index=args.create_index,
        transaction_isolation=args.transaction_isolation,
        cluster_name=args.cluster_name,
        object_count=args.object_count,
    )

    workload_names = [workload.__name__ for workload in workloads]
    df_workloads = pd.DataFrame(data={"workload": workload_names})
    df_workloads.to_csv(RESULTS_DIR / "workloads.csv")

    for workload in workloads:
        assert issubclass(workload, Workload), f"{workload} is not a Workload"
        for endpoint in endpoints:
            run_workload(c, args, endpoint, schema, workload())


def workflow_lab(c: Composition) -> None:
    sys.argv = ["jupyter", "lab", "--no-browser"]
    jupyter_core_command_main()


def workflow_notebook(c: Composition) -> None:
    sys.argv = ["jupyter", "notebook", "--no-browser"]
    jupyter_core_command_main()
