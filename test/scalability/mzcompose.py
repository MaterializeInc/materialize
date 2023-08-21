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
import time
from concurrent import futures
from typing import Any, Optional, Tuple

import pandas as pd
from jupyter_core.command import main as jupyter_core_command_main
from psycopg import Cursor

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Postgres  # noqa: F401
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.endpoints import (
    MaterializeContainer,
    MaterializeLocal,
    MaterializeRemote,
    PostgresContainer,
)
from materialize.scalability.operation import Operation
from materialize.scalability.schema import Schema, TransactionIsolation
from materialize.scalability.workload import Workload
from materialize.scalability.workloads import *  # noqa: F401 F403
from materialize.scalability.workloads_test import *  # noqa: F401 F403

SERVICES = [Materialized(image="materialize/materialized:latest"), Postgres()]


def execute_operation(
    args: Tuple[Workload, int, Operation, Cursor, int, int]
) -> dict[str, Any]:
    workload, concurrency, operation, cursor, i1, i2 = args

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
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    endpoint.up()

    init_sqls = schema.init_sqls()

    init_conn = endpoint.sql_connection()
    init_conn.autocommit = True
    init_cursor = init_conn.cursor()
    for init_sql in init_sqls:
        print(init_sql)
        init_cursor.execute(init_sql)

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
            cursor.execute(connect_sql)
        cursor_pool.append(cursor)

    print(f"Benchmarking workload {type(workload)} at concurrency {concurrency} ...")
    operations = workload.operations()

    start = time.time()
    with futures.ThreadPoolExecutor(concurrency) as executor:
        measurements = executor.map(
            execute_operation,
            [
                (
                    workload,
                    concurrency,
                    operations[i % len(operations)],
                    cursor_pool[i % concurrency],
                    i,
                    i % concurrency,
                )
                for i in range(count)
            ],
        )
    wallclock_total = time.time() - start

    df_detail = pd.DataFrame()
    for measurement in measurements:
        df_detail = pd.concat([df_detail, pd.DataFrame([measurement])])
    print("Best and worst individual measurements:")
    print(df_detail)

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
    for concurrency in range(
        args.min_concurrency, args.max_concurrency + 1, args.concurrency_step
    ):
        df_total, df_detail = run_with_concurrency(
            c, endpoint, schema, workload, concurrency, args.count
        )
        df_totals = pd.concat([df_totals, df_total])
        df_details = pd.concat([df_details, df_detail])

        df_totals.to_csv(f"results/{type(workload).__name__}.csv")
        df_details.to_csv(f"results/{type(workload).__name__}_details.csv")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--target",
        help="Target for the benchmark: 'HEAD', 'local', 'remote', 'Postgres', or a DockerHub tag",
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
        "--concurrency-step", type=int, help="Maximum concurrency to test", default=10
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
        default=2048,
        help="Number of individual operations to benchmark.",
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
        "--materialize-url", type=str, help="URL to connect to for remote targets"
    )

    parser.add_argument("--cluster-name", type=str, help="Cluster to SET CLUSTER to")

    args = parser.parse_args()

    if args.materialize_url is not None and args.target != "remote":
        assert False, "--materialize_url requires --target=remote"

    endpoint: Optional[Endpoint] = None
    if args.target == "local":
        endpoint = MaterializeLocal()
    if args.target == "remote":
        endpoint = MaterializeRemote(materialize_url=args.materialize_url)
    elif args.target == "postgres":
        endpoint = PostgresContainer(composition=c)
    elif args.target == "HEAD" or args.target is None:
        endpoint = MaterializeContainer(composition=c)
    else:
        endpoint = MaterializeContainer(
            composition=c, image=f"materialize/materialized:{args.target}"
        )

    assert endpoint is not None

    workloads = (
        [globals()[workload] for workload in args.workload]
        if args.workload
        else Workload.__subclasses__()
    )

    schema = Schema(
        create_index=args.create_index,
        transaction_isolation=args.transaction_isolation,
        cluster_name=args.cluster_name,
    )

    workload_names = [workload.__name__ for workload in workloads]
    df_workloads = pd.DataFrame(data={"workload": workload_names})
    df_workloads.to_csv("results/workloads.csv")

    for workload in workloads:
        assert issubclass(workload, Workload), f"{workload} is not a Workload"
        run_workload(c, args, endpoint, schema, workload())


def workflow_lab(c: Composition) -> None:
    sys.argv = ["jupyter", "lab", "--no-browser"]
    jupyter_core_command_main()


def workflow_notebook(c: Composition) -> None:
    sys.argv = ["jupyter", "notebook", "--no-browser"]
    jupyter_core_command_main()
