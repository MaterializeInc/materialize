# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Simulates workloads captured via `bin/mz-workload-capture` in a local run using Docker Compose.
"""

from __future__ import annotations

import argparse
import pathlib
import random
import time

import yaml

from materialize import buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.workload_replay.config import cluster_replica_sizes
from materialize.workload_replay.executor import benchmark, test
from materialize.workload_replay.util import (
    get_paths,
    print_workload_stats,
    update_captured_workloads_repo,
)

SERVICES = [
    SshBastionHost(allow_any_key=True),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://127.0.0.1:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Mz(app_password=""),
    Materialized(
        cluster_replica_size=cluster_replica_sizes,
        ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
        environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
        additional_system_parameter_defaults={"enable_rbac_checks": "false"},
    ),
    Testdrive(
        seed=1,
        no_reset=True,
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        ],
    ),
    SqlServer(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--factor-initial-data",
        type=float,
        default=1,
        help="scale factor for initial data generation",
    )
    parser.add_argument(
        "--factor-ingestions",
        type=float,
        default=1,
        help="scale factor for runtime data ingestion rate",
    )
    parser.add_argument(
        "--factor-queries",
        type=float,
        default=1,
        help="scale factor for runtime queries",
    )
    parser.add_argument(
        "--runtime",
        type=int,
        default=1200,
        help="runtime for continuous ingestion/query period, in seconds",
    )
    parser.add_argument(
        "--max-concurrent-queries",
        type=int,
        default=1000,
        help="max. number of concurrent queries during continuous phase",
    )
    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        default=str(int(time.time())),
        help="factor for initial data generation",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--create-objects", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--initial-data", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--early-initial-data",
        action=argparse.BooleanOptionalAction,
        # Seems faster in the existing workloads, the behavior afterwards is
        # about the same since we wait for hydration before continuing
        default=False,
        help="Run the initial data creation before creating sources in Materialize (except for webhooks)",
    )
    parser.add_argument(
        "--run-ingestions", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--run-queries", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    random.seed(args.seed)
    update_captured_workloads_repo()

    files: list[pathlib.Path] = buildkite.shard_list(
        get_paths(args.files),
        lambda file: str(file),
    )

    def run(file: pathlib.Path) -> None:
        with open(file) as f:
            workload = yaml.load(f, Loader=yaml.CSafeLoader)
        # When scale_data is false, use 100% initial data
        settings = workload.get("settings", {})
        factor_initial_data = args.factor_initial_data
        if not settings.get("scale_data", True):
            factor_initial_data = 1.0
        print_workload_stats(file, workload)
        test(
            c,
            workload,
            file,
            factor_initial_data,
            args.factor_ingestions,
            args.factor_queries,
            args.runtime,
            args.verbose,
            args.create_objects,
            args.initial_data,
            args.early_initial_data,
            args.run_ingestions,
            args.run_queries,
            args.max_concurrent_queries,
        )

    c.test_parts(files, run)


def workflow_benchmark(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--factor-initial-data",
        type=float,
        default=1,
        help="scale factor for initial data generation",
    )
    parser.add_argument(
        "--factor-ingestions",
        type=float,
        default=1,
        help="scale factor for runtime data ingestion rate",
    )
    parser.add_argument(
        "--factor-queries",
        type=float,
        default=1,
        help="scale factor for runtime queries",
    )
    parser.add_argument(
        "--runtime",
        type=int,
        default=1200,
        help="runtime for continuous ingestion/query period, in seconds",
    )
    parser.add_argument(
        "--max-concurrent-queries",
        type=int,
        default=1000,
        help="max. number of concurrent queries during continuous phase",
    )
    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        default=str(int(time.time())),
        help="factor for initial data generation",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--compare-against",
        type=str,
        default=None,
        help="compare performance and errors against another Materialize tag",
    )
    parser.add_argument(
        "--early-initial-data",
        action=argparse.BooleanOptionalAction,
        # Seems faster in the existing workloads, the behavior afterwards is
        # about the same since we wait for hydration before continuing
        default=False,
        help="Run the initial data creation before creating sources in Materialize (except for webhooks)",
    )
    parser.add_argument(
        "--skip-without-data-scale",
        action="store_true",
        default=False,
        help="Skip workloads that have scale_data: false in their settings",
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    update_captured_workloads_repo()

    all_paths = get_paths(args.files)
    workloads: dict[pathlib.Path, dict] = {}
    for path in all_paths:
        with open(path) as f:
            workload = yaml.load(f, Loader=yaml.CSafeLoader)
        settings = workload.get("settings", {})
        if not settings.get("scale_data", True) and args.skip_without_data_scale:
            print(f"-- Skipping {path} (scale_data: false)")
            continue
        workloads[path] = workload

    files: list[pathlib.Path] = buildkite.shard_list(
        list(workloads.keys()),
        lambda file: str(file),
    )
    c.test_parts(
        files,
        lambda file: benchmark(
            c,
            file,
            workloads[file],
            args.compare_against,
            args.factor_initial_data,
            args.factor_ingestions,
            args.factor_queries,
            args.runtime,
            args.verbose,
            args.seed,
            args.early_initial_data,
            args.max_concurrent_queries,
        ),
    )


def workflow_stats(c: Composition, parser: WorkflowArgumentParser) -> None:
    with c.override(Materialized(sanity_restart=False)):
        parser.add_argument(
            "files",
            nargs="*",
            default=["*.yml"],
            help="run against the specified files",
        )
        args = parser.parse_args()
        update_captured_workloads_repo()
        for file in get_paths(args.files):
            with open(file) as f:
                workload = yaml.load(f, Loader=yaml.CSafeLoader)
            print()
            print_workload_stats(file, workload)
        print()
