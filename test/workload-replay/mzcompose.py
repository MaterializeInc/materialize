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
from materialize.util import PropagatingThread
from materialize.workload_replay.catalog_server_cost import CatalogServerCost
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


def _materialized(catalog_server_size: str | None = None) -> Materialized:
    environment_extra = ["MZ_NO_BUILTIN_CONSOLE=0"]
    if catalog_server_size:
        environment_extra.append(
            "MZ_BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICA_SIZE="
            + catalog_server_size
        )
    return Materialized(
        cluster_replica_size=cluster_replica_sizes,
        ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
        environment_extra=environment_extra,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
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
    _materialized(),
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
    # Replays the console's captured /api/sql traffic against the SQL HTTP
    # endpoint at increasing virtual-user counts. Reads the queries.json
    # that dump-queries produces during the Playwright suite.
    ServiceDefinition(
        name="console-k6",
        config={
            "image": "grafana/k6:1.6.0",
            "volumes": [f"{MZ_ROOT}:/workdir"],
            "working_dir": "/workdir/console/e2e-tests/k6",
            # The summary export is written into the bind mount.
            "propagate_uid_gid": True,
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
    Run the console Playwright scalability suite against each captured
    workload.

    Per workload: boot (objects, initial data, hydration), then run the
    suite against the console bundled in the materialized image while
    continuous ingestions and queries provide background load. The suite
    only reports timings. Suite failures do not fail the workflow, the runs
    exist to collect data for determining thresholds.

    mz_catalog_server and environmentd CPU is measured for a baseline with
    only the replay running, the suite, the k6 ramp, and with `--viewers`
    each step of a headless viewer sweep. Results land in
    console/test-results/catalog-server-<workload>.json.
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
        "--skip-large",
        action="store_true",
        help="skip workloads tagged `settings.large: true`",
    )
    parser.add_argument(
        "--catalog-server-size",
        choices=sorted(cluster_replica_sizes),
        help="replica size for mz_catalog_server, default is the bootstrap size",
    )
    parser.add_argument(
        "--console-bundle",
        type=lambda value: pathlib.Path(value).resolve(),
        help="console build to serve instead of the one bundled in the "
        "materialized image: the dist directory of a `yarn build` with "
        "CONSOLE_DEPLOYMENT_MODE=flexible-deployment",
    )
    parser.add_argument(
        "--viewers",
        type=lambda value: [int(n) for n in value.split(",")],
        default=[],
        help="comma-separated viewer counts, e.g. 10,25,50: hold that many "
        "headless console viewers per step and fit catalog_server cost per viewer",
    )
    parser.add_argument(
        "--viewer-path",
        default="",
        help="console path the viewers hold, default the mz_catalog_server "
        "cluster page",
    )
    parser.add_argument(
        "--cost-window",
        type=int,
        default=90,
        help="seconds of each catalog_server cost window (baselines, viewer steps)",
    )
    parser.add_argument(
        "--cost-settle",
        type=int,
        default=30,
        help="seconds to wait before each catalog_server cost window",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.yml"],
        help="run against the specified files",
    )
    args = parser.parse_args()
    if args.console_bundle and not (args.console_bundle / "index.html").exists():
        parser.error(f"--console-bundle: no index.html in {args.console_bundle}")

    print(f"-- Random seed is {args.seed}")
    random.seed(args.seed)
    update_captured_workloads_repo()

    all_paths = get_paths(args.files)
    workloads: dict[pathlib.Path, dict] = {}
    for path in all_paths:
        workload = load_workload(path)
        if args.skip_large and workload.get("settings", {}).get("large", False):
            print(f"-- Skipping {path} (settings.large: true and --skip-large)")
            continue
        workloads[path] = workload
    files: list[pathlib.Path] = buildkite.shard_list(
        list(workloads.keys()),
        lambda file: str(file),
    )

    k6_dir = MZ_ROOT / "console" / "e2e-tests" / "k6"

    def run(file: pathlib.Path) -> None:
        workload = workloads[file]
        settings = workload.get("settings", {})
        # When scale_data is false, use 100% initial data, like
        # workflow_default.
        factor_initial_data = args.factor_initial_data
        if not settings.get("scale_data", True):
            factor_initial_data = 1.0
        else:
            factor_initial_data *= settings.get("factor_initial_data_multiplier", 1.0)
        print_workload_stats(file, workload)

        # Reset any state left over from the previous workload.
        service_names = [s.name for s in SERVICES]
        try:
            c.kill(*service_names)
        except Exception:
            pass
        c.rm(*service_names, destroy_volumes=True)
        c.rm_volumes("mzdata", force=True)

        # Prepare the Playwright runner up front so dependency installation
        # does not eat into the measurement window. Cheap after the first
        # workload, node_modules lives in the bind-mounted checkout.
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

        # k6 must only ever replay queries captured from this workload's run,
        # so remove a stale dump.
        queries_json = k6_dir / "queries.json"
        queries_json.unlink(missing_ok=True)

        def run_suite() -> None:
            if args.console_bundle:
                _install_console_bundle(c, args.console_bundle)
            cost = CatalogServerCost(c, file.stem)
            try:
                time.sleep(args.cost_settle)
                cost.window("baseline", args.cost_window)

                print(f"+++ Running console scalability suite ({file.stem})")
                with cost.phase("playwright"):
                    try:
                        _console_runner_sh(
                            c,
                            "yarn test:e2e:scalability --workers=1 --trace=off "
                            f"--output=test-results/{file.stem}",
                            user=_HOST_UID_GID,
                            env={"BASE_URL": "http://materialized:6874"},
                        )
                    except Exception as e:
                        print(
                            "Console scalability suite failed. Not failing the "
                            f"workflow, the suite only collects timing data: {e}"
                        )
                # k6 replays the query set that dump-queries captured above (it
                # runs first in the suite), isolating server-side concurrency
                # from browser behavior.
                if queries_json.exists():
                    print(f"+++ Running console k6 load test ({file.stem})")
                    with cost.phase("k6"):
                        try:
                            c.run(
                                "console-k6",
                                "run",
                                "--summary-export=summary.json",
                                "cluster-detail.ts",
                                rm=True,
                                env_extra={"MZ_HTTP_URL": "http://materialized:6876"},
                            )
                        except Exception as e:
                            print(
                                "Console k6 load test failed. Not failing the "
                                f"workflow, it only collects timing data: {e}"
                            )
                else:
                    print(
                        "dump-queries produced no queries.json, skipping the k6 "
                        "load test"
                    )
                # Keep per-workload copies, the next workload overwrites the
                # originals.
                for name in ("queries", "summary"):
                    src = k6_dir / f"{name}.json"
                    if src.exists():
                        src.rename(k6_dir / f"{name}-{file.stem}.json")

                if args.viewers:
                    _measure_viewers(c, cost, file.stem, args)
            finally:
                cost.report(
                    MZ_ROOT
                    / "console"
                    / "test-results"
                    / f"catalog-server-{file.stem}.json"
                )
                cost.close()

        try:
            test(
                c,
                workload,
                file,
                factor_initial_data,
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
        finally:
            # The runner idles on sleep infinity, don't let it outlive the
            # workflow into CI teardown.
            try:
                c.kill("console-scalability-runner")
                c.rm("console-scalability-runner")
            except Exception:
                pass

    with c.override(_materialized(args.catalog_server_size)):
        c.test_parts(files, run)


def _install_console_bundle(c: Composition, bundle: pathlib.Path) -> None:
    """Serve `bundle` in place of the console bundled in the materialized
    image, which measures a console change without rebuilding materialized."""
    print(f"-- Serving the console from {bundle}")
    # The bundled console is owned by root, the container runs as materialize.
    c.invoke(
        "exec",
        "--user",
        "root",
        "-T",
        "materialized",
        "sh",
        "-c",
        "rm -rf /usr/share/nginx/html/*",
    )
    c.invoke("cp", f"{bundle}/.", "materialized:/usr/share/nginx/html/")


def _measure_viewers(
    c: Composition, cost: CatalogServerCost, stem: str, args: argparse.Namespace
) -> None:
    """Hold each count in `args.viewers` of headless console viewers and
    measure catalog_server while they are open.

    The viewers spec talks to this function through files in the bind-mounted
    checkout: it creates the ready file once every viewer has loaded its page,
    and closes the viewers when the stop file appears.
    """
    control = f"test-results/viewers-{stem}"
    host_control = MZ_ROOT / "console" / control
    host_control.mkdir(parents=True, exist_ok=True)
    ready, stop = host_control / "ready", host_control / "stop"

    def hold(viewers: int) -> None:
        env = {
            "BASE_URL": "http://materialized:6874",
            "VIEWERS": str(viewers),
            "VIEWERS_READY_FILE": f"{control}/ready",
            "VIEWERS_STOP_FILE": f"{control}/stop",
        }
        if args.viewer_path:
            env["VIEWER_URL"] = "http://materialized:6874" + args.viewer_path
        try:
            # A separate output directory, since Playwright empties its
            # output directory on start and the control files live above it.
            _console_runner_sh(
                c,
                "yarn test:e2e:scalability viewers.spec.ts --workers=1 "
                f"--trace=off --output={control}/playwright-{viewers}",
                user=_HOST_UID_GID,
                env=env,
            )
        except Exception as e:
            print(f"Console viewers failed, no reading for {viewers} viewers: {e}")

    time.sleep(args.cost_settle)
    cost.window("baseline-before-viewers", args.cost_window)
    baseline_subscriptions = cost.subscriptions()
    for viewers in args.viewers:
        ready.unlink(missing_ok=True)
        stop.unlink(missing_ok=True)
        print(f"+++ Holding {viewers} console viewers ({stem})")
        thread = PropagatingThread(target=hold, args=(viewers,))
        thread.start()
        while not ready.exists() and thread.is_alive():
            time.sleep(1)
        if ready.exists():
            time.sleep(args.cost_settle)
            cost.window(f"viewers-{viewers}", args.cost_window, viewers=viewers)
        stop.touch()
        thread.join()
        # Let the closed viewers' subscribes drain so the next step starts
        # from the baseline.
        deadline = time.time() + 60
        while cost.subscriptions() > baseline_subscriptions and time.time() < deadline:
            time.sleep(2)
    time.sleep(args.cost_settle)
    cost.window("baseline-after-viewers", args.cost_window)
    cost.fit_viewers(["baseline-before-viewers", "baseline-after-viewers"])


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
