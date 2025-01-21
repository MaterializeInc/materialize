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
import atexit
import getpass
import json
import os
import pathlib
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from urllib.parse import parse_qsl, urlparse

import pg8000.exceptions
import pg8000.native
import psutil

from materialize import MZ_ROOT, rustc_flags, spawn, ui
from materialize.bazel import remote_cache_arg
from materialize.build_config import BuildConfig
from materialize.mzcompose import (
    bootstrap_cluster_replica_size,
    cluster_replica_size_map,
    get_default_system_parameters,
)
from materialize.ui import UIError
from materialize.xcompile import Arch

KNOWN_PROGRAMS = ["environmentd", "sqllogictest"]
REQUIRED_SERVICES = ["clusterd"]

SANITIZER_TARGET = (
    f"{Arch.host()}-unknown-linux-gnu"
    if sys.platform.startswith("linux")
    else f"{Arch.host()}-apple-darwin"
)
DEFAULT_POSTGRES = "postgres://root@localhost:26257/materialize"
MZDATA = MZ_ROOT / "mzdata"
DEFAULT_BLOB = f"file://{MZDATA}/persist/blob"

# sets entitlements on the built binary, e.g. environmentd, so you can inspect it with Instruments
MACOS_ENTITLEMENTS_DATA = """
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
    <dict>
        <key>com.apple.security.get-task-allow</key>
        <true/>
    </dict>
</plist>
"""


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
        "--blob",
        help="Blob storage connection string",
        default=os.getenv("MZDEV_BLOB", DEFAULT_BLOB),
    )
    parser.add_argument(
        "--release",
        help="Build artifacts in release mode, with optimizations",
        action="store_true",
    )
    parser.add_argument(
        "--optimized",
        help="Build artifacts in optimized mode, with optimizations (but no LTO and debug symbols)",
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
    parser.add_argument(
        "--enable-mac-codesigning",
        help="Enables the limited codesigning we do on macOS to support Instruments",
        action="store_true",
    )
    parser.add_argument(
        "--coverage",
        help="Build with coverage",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--sanitizer",
        help="Build with sanitizer",
        type=str,
        default="none",
    )
    parser.add_argument(
        "--wrapper",
        help="Wrapper command for the program",
    )
    parser.add_argument(
        "--monitoring",
        help="Automatically send monitoring data.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--bazel",
        help="Build with Bazel (not supported by all options)",
        default=False,
        action="store_true",
    )
    args = parser.parse_intermixed_args()

    # Handle `+toolchain` like rustup.
    args.channel = None
    if len(args.args) > 0 and args.args[0].startswith("+"):
        args.channel = args.args[0]
        del args.args[0]

    env = dict(os.environ)
    if args.program in KNOWN_PROGRAMS:
        if args.bazel:
            build_func = _bazel_build
        else:
            build_func = _cargo_build

        (build_retcode, built_programs) = build_func(
            args, extra_programs=[args.program]
        )

        if args.build_only:
            return build_retcode

        if args.enable_mac_codesigning:
            for program in built_programs:
                if sys.platform == "darwin":
                    _macos_codesign(program)
            if sys.platform != "darwin":
                print("Ignoring --enable-mac-codesigning since we're not on macOS")
        else:
            print("Disabled macOS Codesigning")

        if args.wrapper:
            command = shlex.split(args.wrapper)
        else:
            command = []

        mzbuild = MZ_ROOT / "mzbuild"
        mzbuild.mkdir(exist_ok=True)

        # Move all built programs into the same directory to make it easier for
        # downstream consumers.
        binaries_dir = mzbuild / "bin"
        binaries_dir.mkdir(exist_ok=True)
        binaries = []
        for program in built_programs:
            src_path = pathlib.Path(program)
            dst_path = binaries_dir / src_path.name
            if os.path.lexists(dst_path) and os.path.islink(dst_path):
                os.unlink(dst_path)
            os.symlink(src_path, dst_path)
            binaries.append(str(dst_path))

        # HACK(parkmycar): The last program is the one requested by the user.
        command.append(binaries[-1])
        if args.tokio_console:
            command += ["--tokio-console-listen-addr=127.0.0.1:6669"]
        if args.program == "environmentd":
            _handle_lingering_services(kill=args.reset)
            scratch = MZ_ROOT / "scratch"
            urlparse(args.postgres).path.removeprefix("/")
            dbconn = _connect_sql(args.postgres)
            for schema in ["consensus", "tsoracle", "storage"]:
                if args.reset:
                    _run_sql(dbconn, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                _run_sql(dbconn, f"CREATE SCHEMA IF NOT EXISTS {schema}")
            # Keep this after clearing out Postgres. Otherwise there is a race
            # where a ctrl-c could leave persist with references in Postgres to
            # files that have been deleted. There's no race if we reset in the
            # opposite order.
            if args.reset:
                # Remove everything in the `mzdata`` directory *except* for
                # the `prometheus` directory and all contents of `tempo`.
                paths = list(MZDATA.glob("prometheus/*"))
                paths.extend(
                    p
                    for p in MZDATA.glob("*")
                    if p.name != "prometheus" and p.name != "tempo"
                )
                paths.extend(p for p in scratch.glob("*"))
                for path in paths:
                    print(f"Removing {path}...")
                    if path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        path.unlink()

            MZDATA.mkdir(exist_ok=True)
            scratch.mkdir(exist_ok=True)
            environment_file = MZDATA / "environment-id"
            try:
                environment_id = environment_file.read_text().rstrip()
            except FileNotFoundError:
                environment_id = f"local-az1-{uuid.uuid4()}-0"
                environment_file.write_text(environment_id)

            print(f"persist-blob-url: {args.blob}")
            command += [
                # Setting the listen addresses below to 0.0.0.0 is required
                # to allow Prometheus running in Docker (misc/prometheus)
                # access these services to scrape metrics.
                "--internal-http-listen-addr=0.0.0.0:6878",
                "--orchestrator=process",
                f"--orchestrator-process-secrets-directory={MZDATA}/secrets",
                "--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0",
                f"--orchestrator-process-prometheus-service-discovery-directory={MZDATA}/prometheus",
                f"--orchestrator-process-scratch-directory={scratch}",
                "--secrets-controller=local-file",
                f"--persist-consensus-url={args.postgres}?options=--search_path=consensus",
                f"--persist-blob-url={args.blob}",
                f"--timestamp-oracle-url={args.postgres}?options=--search_path=tsoracle",
                f"--environment-id={environment_id}",
                "--bootstrap-role=materialize",
                f"--cluster-replica-sizes={json.dumps(cluster_replica_size_map())}",
                f"--bootstrap-default-cluster-replica-size={bootstrap_cluster_replica_size()}",
                *args.args,
            ]
            if args.monitoring:
                command += ["--opentelemetry-endpoint=http://localhost:4317"]
        elif args.program == "sqllogictest":
            formatted_params = [
                f"{key}={value}"
                for key, value in get_default_system_parameters().items()
            ]
            system_parameter_default = ";".join(formatted_params)
            # Connect to the database to ensure it exists.
            _connect_sql(args.postgres)
            command += [
                f"--postgres-url={args.postgres}",
                f"--system-parameter-default={system_parameter_default}",
                *args.args,
            ]
    elif args.program == "test":
        if args.bazel:
            raise UIError(
                "testing with Bazel is not yet supported, go bug Parker about it"
            )

        (build_retcode, _) = _cargo_build(args)
        if args.build_only:
            return build_retcode

        command = _cargo_command(args, "nextest")
        try:
            subprocess.check_output(
                command + ["--version"], env=env, stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError:
            raise UIError("cargo nextest not found, run `cargo install cargo-nextest`")

        command += ["run"]

        for package in args.package:
            command += ["--package", package]
        for test in args.test:
            command += ["--test", test]
        command += args.args
        env["COCKROACH_URL"] = args.postgres
        # some tests run into stack overflows
        env["RUST_MIN_STACK"] = "4194304"
        dbconn = _connect_sql(args.postgres)
    else:
        raise UIError(f"unknown program {args.program}")

    # We fork off a process that moves to its own process group and then runs
    # `command`. This parent process continues running until `command` exits,
    # and then kills `command`'s process group. This avoids leaking "grandchild"
    # processes--e.g., if we spawn an `environmentd` process that in turn spawns
    # `clusterd` processes, and then `environmentd` panics, we want to clean up
    # those `clusterd` processes before we exit.
    #
    # This isn't foolproof. If this script itself crashes, that can leak
    # processes. The subprocess can also intentionally daemonize (i.e., move to
    # another process group) to evade our detection. But this catches the vast
    # majority of cases and is simple to reason about.

    child_pid = os.fork()
    assert child_pid >= 0
    if child_pid > 0:
        # This is the parent process, responsible for cleaning up after the
        # child.

        # First, arrange to terminate all processes in the child's process group
        # when we exit.
        def _kill_childpg():
            try:
                os.killpg(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

        atexit.register(_kill_childpg)

        # Wait for the child to exit then propagate its exit status.
        _, ws = os.waitpid(child_pid, 0)
        exit(os.waitstatus_to_exitcode(ws))

    # This is the child. First, move to a dedicated process group.
    os.setpgid(child_pid, child_pid)

    # Then, spawn the desired command.
    print(f"$ {' '.join(command)}")
    if args.program == "environmentd":
        # Automatically restart `environmentd` after it halts, but not more than
        # once every 5s to prevent hot loops. This simulates what happens when
        # running in Kubernetes, which restarts failed `environmentd` process
        # automatically. (We don't restart after a panic, since panics are
        # generally unexpected and we don't want to inadvertently hide them
        # during local development.)
        while True:
            last_start_time = datetime.now()
            proc = subprocess.run(command, env=env)
            if proc.returncode == 2:
                wait = max(
                    timedelta(seconds=5) - (datetime.now() - last_start_time),
                    timedelta(seconds=0),
                )
                print(f"environmentd halted; will restart in {wait.total_seconds()}s")
                time.sleep(wait.total_seconds())
            else:
                break
    else:
        proc = subprocess.run(command, env=env)

    exit(proc.returncode)


def _bazel_build(
    args: argparse.Namespace, extra_programs: list[str] = []
) -> tuple[int, list[str]]:
    config = BuildConfig.read()
    command = _bazel_command(args, config, ["build"])

    programs = [*REQUIRED_SERVICES, *extra_programs]
    targets = [_bazel_target(program) for program in programs]
    command += targets

    completed_proc = spawn.runv(command)
    artifacts = [str(_bazel_artifact_path(args, t)) for t in targets]

    return (completed_proc.returncode, artifacts)


def _bazel_target(program: str) -> str:
    if program == "environmentd":
        return "//src/environmentd:environmentd"
    elif program == "clusterd":
        return "//src/clusterd:clusterd"
    elif program == "sqllogictest":
        return "//src/sqllogictest:sqllogictest"
    else:
        raise UIError(f"unknown program {program}")


def _bazel_command(
    args: argparse.Namespace, config: BuildConfig | None, subcommands: list[str]
) -> list[str]:
    command = ["bazel"] + subcommands

    if args.release:
        command += ["--config=optimized"]
    if config:
        command += remote_cache_arg(config)

    return command


def _bazel_artifact_path(args: argparse.Namespace, program: str) -> pathlib.Path:
    cmd = _bazel_command(args, None, ["cquery", "--output=files"]) + [program]
    raw_path = subprocess.check_output(cmd, text=True)
    relative_path = pathlib.Path(raw_path.strip())
    return MZ_ROOT / relative_path


def _cargo_build(
    args: argparse.Namespace, extra_programs: list[str] = []
) -> tuple[int, list[str]]:
    env = dict(os.environ)
    command = _cargo_command(args, "build")
    features = []

    if args.coverage:
        env["RUSTFLAGS"] = (
            env.get("RUSTFLAGS", "") + " " + " ".join(rustc_flags.coverage)
        )
    if args.sanitizer != "none":
        env["RUSTFLAGS"] = (
            env.get("RUSTFLAGS", "")
            + " "
            + " ".join(rustc_flags.sanitizer[args.sanitizer])
        )
        env["CFLAGS"] = (
            env.get("CFLAGS", "")
            + " "
            + " ".join(rustc_flags.sanitizer_cflags[args.sanitizer])
        )
        env["CXXFLAGS"] = (
            env.get("CXXFLAGS", "")
            + " "
            + " ".join(rustc_flags.sanitizer_cflags[args.sanitizer])
        )
        env["LDFLAGS"] = (
            env.get("LDFLAGS", "")
            + " "
            + " ".join(rustc_flags.sanitizer_cflags[args.sanitizer])
        )
    if args.features:
        features.extend(args.features.split(","))
    if features:
        command += [f"--features={','.join(features)}"]

    programs = [*REQUIRED_SERVICES, *extra_programs]
    for program in programs:
        command += ["--bin", program]
    completed_proc = spawn.runv(command, env=env)

    artifacts = [str(_cargo_artifact_path(args, program)) for program in programs]

    return (completed_proc.returncode, artifacts)


def _cargo_command(args: argparse.Namespace, subcommand: str) -> list[str]:
    command = ["cargo"]
    if args.channel:
        command += [args.channel]
    command += [subcommand]
    if args.release:
        command += ["--release"]
    if args.optimized:
        command += ["--profile", "optimized"]
    if args.timings:
        command += ["--timings"]
    if args.no_default_features:
        command += ["--no-default-features"]
    if args.sanitizer != "none":
        command += ["-Zbuild-std", "--target", SANITIZER_TARGET]
    return command


def _cargo_artifact_path(args: argparse.Namespace, program: str) -> pathlib.Path:
    dir_name = "release" if args.release else "optimized" if args.optimized else "debug"
    if args.sanitizer != "none":
        artifact_path = MZ_ROOT / "target" / SANITIZER_TARGET / dir_name
    else:
        artifact_path = MZ_ROOT / "target" / dir_name

    return artifact_path / program


def _macos_codesign(path: str) -> None:
    env = dict(os.environ)
    command = ["codesign"]
    command.extend(["-s", "-", "-f", "--entitlements"])

    # write our entitlements file to a temp path
    temp = tempfile.NamedTemporaryFile()
    temp.write(bytes(MACOS_ENTITLEMENTS_DATA, "utf-8"))
    temp.flush()

    command.append(temp.name)
    command.append(path)

    spawn.runv(command, env=env)


def _connect_sql(urlstr: str) -> pg8000.native.Connection:
    hint = """Have you correctly configured CockroachDB or PostgreSQL?

For CockroachDB:
    Follow the instructions in doc/developer/guide.md#CockroachDB

For PostgreSQL:
    1. Install PostgreSQL
    2. Create a database: `createdb materialize`
    3. Set the MZDEV_POSTGRES environment variable accordingly: `export MZDEV_POSTGRES=postgres://localhost/materialize`"""
    url = urlparse(urlstr)
    database = url.path.removeprefix("/")
    try:
        dbconn = pg8000.native.Connection(
            host=url.hostname or "localhost",
            port=url.port or 5432,
            user=url.username or getpass.getuser(),
            password=url.password,
            database=database,
            startup_params={key: value for key, value in parse_qsl(url.query)},
        )
    except pg8000.exceptions.DatabaseError as e:
        raise UIError(
            f"unable to connect to metadata database: {e.args[0]['M']}",
            hint=hint,
        )
    except pg8000.exceptions.InterfaceError as e:
        raise UIError(
            f"unable to connect to metadata database: {e}",
            hint=hint,
        )

    # For CockroachDB, after connecting, we can ensure the database exists. For
    # PostgreSQL, the database must exist for us to connect to it at all--we
    # declare it to be the user's problem to create this database.
    if "crdb_version" in dbconn.parameter_statuses:
        if not database:
            raise UIError(
                f"database name is missing in the postgres URL: {urlstr}",
                hint="When connecting to CockroachDB, the database name is required.",
            )
        _run_sql(dbconn, f"CREATE DATABASE IF NOT EXISTS {database}")

    return dbconn


def _run_sql(conn: pg8000.native.Connection, sql: str) -> None:
    print(f"> {sql}")
    conn.run(sql)


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
