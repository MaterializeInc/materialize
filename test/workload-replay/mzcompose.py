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

import argparse
import os
import pathlib
import random
import time

import yaml

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as ServiceDefinition
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.workload_replay.config import (
    additional_system_parameter_defaults,
    cluster_replica_sizes,
)
from materialize.workload_replay.executor import benchmark, test
from materialize.workload_replay.util import (
    get_paths,
    load_workload,
    print_workload_stats,
    update_captured_workloads_repo,
)

SERVICES = [
    SshBastionHost(allow_any_key=True),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        advertised_listeners=[
            "HOST://127.0.0.1:30123",
            "PLAINTEXT://kafka:9092",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Minio(),
    Mc(),
    PolarisBootstrap(),
    Polaris(),
    Mz(app_password=""),
    Materialized(
        cluster_replica_size=cluster_replica_sizes,
        ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
        environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
        additional_system_parameter_defaults=additional_system_parameter_defaults,
    ),
    Testdrive(
        seed=1,
        no_reset=True,
        no_consistency_checks=True,
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        ],
    ),
    SqlServer(),
    # Runs the console Playwright scalability suite against the console
    # bundled in the materialized image. The image tag must match the
    # @playwright/test version in console/package.json so the preinstalled
    # browsers are compatible.
    ServiceDefinition(
        name="console-scalability-runner",
        config={
            "image": "mcr.microsoft.com/playwright:v1.61.0-noble",
            "volumes": [f"{MZ_ROOT}:/workdir"],
            "working_dir": "/workdir/console",
            "init": True,
            # Chromium crashes with the 64MB docker default for /dev/shm.
            "ipc": "host",
        },
    ),
]

# UID/GID of the host user, used to run yarn commands so they don't create
# root-owned files in the bind-mounted volume.
_HOST_UID_GID = f"{os.getuid()}:{os.getgid()}"


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
        default=420,
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
    parser.add_argument(
        "--skip-large",
        action="store_true",
        help="skip workloads tagged `settings.large: true` (e.g. nightly skips them but release-qualification does not)",
    )
    parser.add_argument(
        "--skip-without-data-scale",
        action="store_true",
        default=False,
        help="Skip workloads that have scale_data: false in their settings",
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
        default=True,
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
        workload = load_workload(file)
        settings = workload.get("settings", {})
        if args.skip_large and settings.get("large", False):
            print(f"-- Skipping {file} (settings.large: true and --skip-large)")
            return
        if args.skip_without_data_scale and not settings.get("scale_data", True):
            print(f"-- Skipping {file} (scale_data: false)")
            return

        # Anonymized captures reuse object names (cluster_0, db_0, ...) across
        # files and test() does not clean up between them, so reset all state
        # first. Otherwise the second file fails on duplicate CREATE
        # CLUSTER/DATABASE. Resetting before each run also recovers cleanly when
        # a previous file failed partway through.
        service_names = [s.name for s in SERVICES]
        try:
            c.kill(*service_names)
        except Exception:
            pass
        c.rm(*service_names, destroy_volumes=True)
        c.rm_volumes("mzdata", force=True)

        # When scale_data is false, use 100% initial data
        factor_initial_data = args.factor_initial_data
        if not settings.get("scale_data", True):
            factor_initial_data = 1.0
        else:
            # A workload can shrink itself further via
            # `settings.factor_initial_data_multiplier` — useful when a single
            # captured workload is dramatically larger than the rest and would
            # blow past the CI timeout at the global factor.
            factor_initial_data *= settings.get("factor_initial_data_multiplier", 1.0)
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
        default=420,
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
        default=True,
        help="Run the initial data creation before creating sources in Materialize (except for webhooks)",
    )
    parser.add_argument(
        "--skip-without-data-scale",
        action="store_true",
        default=False,
        help="Skip workloads that have scale_data: false in their settings",
    )
    parser.add_argument(
        "--skip-large",
        action="store_true",
        help="skip workloads tagged `settings.large: true` (e.g. nightly skips them but release-qualification does not)",
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    update_captured_workloads_repo()

    all_paths = get_paths(args.files)
    workloads: dict[pathlib.Path, dict] = {}
    for path in all_paths:
        workload = load_workload(path)
        settings = workload.get("settings", {})
        if not settings.get("scale_data", True) and args.skip_without_data_scale:
            print(f"-- Skipping {path} (scale_data: false)")
            continue
        if settings.get("large", False) and args.skip_large:
            print(f"-- Skipping {path} (settings.large: true)")
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


def workflow_console_scalability(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Run the console Playwright scalability suite against a captured workload.

    Boots the workload (objects, initial data, hydration), then runs the
    suite against the console bundled in the materialized image while
    continuous ingestions and queries provide background load. The suite
    only reports timings. Suite failures do not fail the workflow, the runs
    exist to collect data for determining thresholds.
    """
    parser.add_argument(
        "--factor-initial-data",
        type=float,
        default=0.01,
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
        help="random seed",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "files",
        nargs="*",
        default=["workload_prod_analytics.yml"],
        help="the workload to run against (must match exactly one file)",
    )
    args = parser.parse_args()

    print(f"-- Random seed is {args.seed}")
    random.seed(args.seed)
    update_captured_workloads_repo()

    files = get_paths(args.files)
    if len(files) != 1:
        raise ValueError(
            f"console-scalability expects exactly one workload, got {[str(f) for f in files]}"
        )
    file = files[0]
    workload = load_workload(file)
    print_workload_stats(file, workload)

    # Reset any state left over from a previous run, like workflow_default.
    service_names = [s.name for s in SERVICES]
    try:
        c.kill(*service_names)
    except Exception:
        pass
    c.rm(*service_names, destroy_volumes=True)
    c.rm_volumes("mzdata", force=True)

    # Prepare the Playwright runner up front so dependency installation does
    # not eat into the measurement window.
    c.up(Service("console-scalability-runner", idle=True))
    c.exec(
        "console-scalability-runner",
        "sh",
        "-c",
        "corepack enable",
        env_extra={"COREPACK_ENABLE_DOWNLOAD_PROMPT": "0"},
    )
    _console_runner_sh(
        c, "yarn install --immutable --network-timeout 30000", user=_HOST_UID_GID
    )
    # Fetches matching browsers in case @playwright/test drifted from the
    # image tag. Runs as root because it writes to /ms-playwright.
    _console_runner_sh(c, "node_modules/.bin/playwright install chromium")

    def run_suite() -> None:
        print("+++ Running console scalability suite")
        try:
            _console_runner_sh(
                c,
                "yarn test:e2e:scalability --workers=1 --trace=off",
                user=_HOST_UID_GID,
                env={"BASE_URL": "http://materialized:6874"},
            )
        except Exception as e:
            print(
                "Console scalability suite failed. Not failing the workflow, "
                f"the suite only collects timing data: {e}"
            )

    test(
        c,
        workload,
        file,
        args.factor_initial_data,
        args.factor_ingestions,
        args.factor_queries,
        0,  # runtime is unused, the suite governs the continuous phase
        args.verbose,
        True,  # create_objects
        True,  # initial_data
        True,  # early_initial_data
        True,  # run_ingestions
        True,  # run_queries
        args.max_concurrent_queries,
        during_continuous=run_suite,
    )


def _console_runner_sh(
    c: Composition,
    command: str,
    user: str | None = None,
    env: dict[str, str] | None = None,
) -> None:
    """Run a shell command in the console-scalability-runner service."""
    full_env = {
        "COREPACK_ENABLE_DOWNLOAD_PROMPT": "0",
        # yarn needs a writable HOME when running as the host user.
        "HOME": "/tmp",
        **(env or {}),
    }
    c.invoke(
        "exec",
        *(["--user", user] if user else []),
        *(f"-e{k}={v}" for k, v in full_env.items()),
        "-T",
        "console-scalability-runner",
        "sh",
        "-c",
        command,
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
