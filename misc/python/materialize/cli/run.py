# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# run.py — build and run a core service or test.

import argparse
import os
import shutil
import uuid
from urllib.parse import urlparse

import psutil

from materialize import ROOT, spawn, ui
from materialize.ui import UIError

KNOWN_PROGRAMS = ["environmentd", "sqllogictest"]
REQUIRED_SERVICES = ["clusterd"]

DEFAULT_POSTGRES = f"postgres://root@localhost:26257/materialize"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run",
        description="""Build and run a core service or test.
        Wraps `cargo run` and `cargo test` with Materialize-specific logic.""",
    )
    parser.add_argument(
        "program",
        help="the name of the program to run",
        choices=[*KNOWN_PROGRAMS, "test"],
    )
    parser.add_argument(
        "args",
        help="Arguments to pass to the program",
        nargs="*",
    )
    parser.add_argument(
        "--reset",
        help="Delete data from prior runs of the program",
        action="store_true",
    )
    parser.add_argument(
        "--postgres",
        help="Postgres/CockroachDB connection string",
        default=os.getenv("MZDEV_POSTGRES", DEFAULT_POSTGRES),
    )
    parser.add_argument(
        "--release",
        help="Build artifacts in release mode, with optimizations",
        action="store_true",
    )
    parser.add_argument(
        "--timings",
        help="Output timing information",
        action="store_true",
    )
    parser.add_argument(
        "--features",
        help="Comma separated list of features to activate",
    )
    parser.add_argument(
        "--no-default-features",
        help="Do not activate the `default` feature",
        action="store_true",
    )
    parser.add_argument(
        "-p",
        "--package",
        help="Package to run tests for",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--test",
        help="Test only the specified test target",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--tokio-console",
        help="Activate the Tokio console",
        action="store_true",
    )
    parser.add_argument(
        "--build-only",
        help="Only build, don't run",
        action="store_true",
    )
    args = parser.parse_intermixed_args()

    # Handle `+toolchain` like rustup.
    args.channel = None
    if len(args.args) > 0 and args.args[0].startswith("+"):
        args.channel = args.args[0]
        del args.args[0]

    if args.program in KNOWN_PROGRAMS:
        build_retcode = _build(args, extra_programs=[args.program])
        if args.build_only:
            return build_retcode

        if args.release:
            path = ROOT / "target" / "release" / args.program
        else:
            path = ROOT / "target" / "debug" / args.program
        command = [str(path)]
        if args.tokio_console:
            command += ["--tokio-console-listen-addr=127.0.0.1:6669"]
        if args.program == "environmentd":
            _handle_lingering_services(kill=args.reset)
            mzdata = ROOT / "mzdata"
            db = urlparse(args.postgres).path.removeprefix("/")
            _run_sql(args.postgres, f"CREATE DATABASE IF NOT EXISTS {db}")
            for schema in ["consensus", "adapter", "storage"]:
                if args.reset:
                    _run_sql(args.postgres, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                _run_sql(args.postgres, f"CREATE SCHEMA IF NOT EXISTS {schema}")
            # Keep this after clearing out Postgres. Otherwise there is a race
            # where a ctrl-c could leave persist with references in Postgres to
            # files that have been deleted. There's no race if we reset in the
            # opposite order.
            if args.reset:
                # Remove everything in the `mzdata`` directory *except* for
                # the `prometheus` directory.
                paths = list(mzdata.glob("prometheus/*"))
                paths.extend(p for p in mzdata.glob("*") if p.name != "prometheus")
                for path in paths:
                    print(f"Removing {path}...")
                    if path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        path.unlink()

            mzdata.mkdir(exist_ok=True)
            environment_file = mzdata / "environment-id"
            try:
                environment_id = environment_file.read_text().rstrip()
            except FileNotFoundError:
                environment_id = f"local-az1-{uuid.uuid4()}-0"
                environment_file.write_text(environment_id)

            command += [
                # Setting the listen addresses below to 0.0.0.0 is required
                # to allow Prometheus running in Docker (misc/prometheus)
                # access these services to scrape metrics.
                f"--internal-http-listen-addr=0.0.0.0:6878",
                f"--orchestrator=process",
                f"--orchestrator-process-secrets-directory={mzdata}/secrets",
                f"--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0",
                f"--orchestrator-process-prometheus-service-discovery-directory={mzdata}/prometheus",
                f"--persist-consensus-url={args.postgres}?options=--search_path=consensus",
                f"--persist-blob-url=file://{mzdata}/persist/blob",
                f"--adapter-stash-url={args.postgres}?options=--search_path=adapter",
                f"--storage-stash-url={args.postgres}?options=--search_path=storage",
                f"--environment-id={environment_id}",
                *args.args,
            ]
        elif args.program == "sqllogictest":
            command += [f"--postgres-url={args.postgres}"]
    elif args.program == "test":
        build_retcode = _build(args)
        if args.build_only:
            return build_retcode

        command = _cargo_command(args, "test")
        for package in args.package:
            command += ["--package", package]
        for test in args.test:
            command += ["--test", test]
        command += args.args
        command += ["--", "--nocapture"]
        os.environ["COCKROACH_URL"] = args.postgres
    else:
        raise UIError(f"unknown program {args.program}")

    print(f"$ {' '.join(command)}")
    os.execvp(command[0], command)


def _build(args: argparse.Namespace, extra_programs: list[str] = []) -> int:
    env = dict(os.environ)
    command = _cargo_command(args, "build")
    features = []
    if args.tokio_console:
        features += ["tokio-console"]
        env["RUSTFLAGS"] = env.get("RUSTFLAGS", "") + " --cfg=tokio_unstable"
    if args.features:
        features.extend(args.features.split(","))
    if features:
        command += [f"--features={','.join(features)}"]
    for program in [*REQUIRED_SERVICES, *extra_programs]:
        command += ["--bin", program]
    completed_proc = spawn.runv(command, env=env)
    return completed_proc.returncode


def _cargo_command(args: argparse.Namespace, subcommand: str) -> list[str]:
    command = ["cargo"]
    if args.channel:
        command += [args.channel]
    command += [subcommand]
    if args.release:
        command += ["--release"]
    if args.timings:
        command += ["--timings"]
    if args.no_default_features:
        command += ["--no-default-features"]
    return command


def _run_sql(url: str, sql: str) -> None:
    try:
        spawn.runv(["psql", "-At", url, "-c", sql])
    except Exception as e:
        raise UIError(
            f"unable to execute postgres statement: {e}",
            hint="Have you installed and started CockroachDB?",
        )


def _handle_lingering_services(kill: bool = False) -> None:
    uid = os.getuid()
    for proc in psutil.process_iter():
        try:
            if proc.name() in REQUIRED_SERVICES:
                if proc.uids().real != uid:
                    print(
                        f"Ignoring {proc.name()} process with different UID (PID {proc.pid}, likely running in Docker)"
                    )
                elif kill:
                    print(f"Killing orphaned {proc.name()} process (PID {proc.pid})")
                    proc.kill()
                else:
                    ui.warn(
                        f"Existing {proc.name()} process (PID {proc.pid}) will be reused"
                    )
        except psutil.NoSuchProcess:
            continue


if __name__ == "__main__":
    with ui.error_handler("run"):
        main()
