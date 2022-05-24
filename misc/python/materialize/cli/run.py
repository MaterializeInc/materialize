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
import getpass
import os
import sys

from materialize import ROOT, spawn, ui
from materialize.ui import UIError

KNOWN_PROGRAMS = ["materialized", "sqllogictest"]
REQUIRED_SERVICES = ["storaged", "computed"]

if sys.platform == "darwin":
    DEFAULT_POSTGRES = f"postgres://{getpass.getuser()}@%2Ftmp"
else:
    DEFAULT_POSTGRES = f"postgres://{getpass.getuser()}@%2Fvar%2Frun%2Fpostgresql"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run",
        description="""Build and run a core service or test.
        Wraps `cargo run` and `cargo test` with Materialize-specific logic.""",
    )
    parser.add_argument(
        "program",
        help="the name of the program to run",
        choices=["materialized", "sqllogictest", "test"],
    )
    parser.add_argument(
        "args",
        help="Arguments to pass to the program",
        nargs="*",
    )
    parser.add_argument(
        "--postgres",
        help="PostgreSQL connection string",
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
        "--no-default-features",
        help="Do not activate the `default` feature",
        action="store_true",
    )
    parser.add_argument(
        "--tokio-console",
        help="Activate the Tokio console",
        action="store_true",
    )
    args = parser.parse_args()

    # Handle `+toolchain` like rustup.
    args.channel = None
    if len(args.args) > 0 and args.args[0].startswith("+"):
        args.channel = args.args[0]
        del args.args[0]

    if args.program in KNOWN_PROGRAMS:
        _build(args, extra_programs=[args.program])
        if args.release:
            path = ROOT / "target" / "release" / args.program
        else:
            path = ROOT / "target" / "debug" / args.program
        command = [str(path), *args.args]
        if args.tokio_console:
            command += ["--tokio-console"]
        if args.program == "materialized":
            _run_sql(args.postgres, "CREATE SCHEMA IF NOT EXISTS consensus")
            _run_sql(args.postgres, "CREATE SCHEMA IF NOT EXISTS catalog")
            command += [
                f"--persist-consensus-url={args.postgres}?options=--search_path=consensus",
                f"--catalog-postgres-stash={args.postgres}?options=--search_path=catalog",
            ]
    elif args.program == "test":
        _build(args)
        command = _cargo_command(args, "test")
        command += args.args
    else:
        raise UIError(f"unknown program {args.program}")

    print(f"$ {' '.join(command)}")
    os.execvp(command[0], command)


def _build(args: argparse.Namespace, extra_programs: list[str] = []) -> None:
    env = dict(os.environ)
    command = _cargo_command(args, "build")
    if args.tokio_console:
        command += ["--features=tokio-console"]
        env["RUSTFLAGS"] = env.get("RUSTFLAGS", "") + " --cfg=tokio_unstable"
    for program in [*REQUIRED_SERVICES, *extra_programs]:
        command += ["--bin", program]
    spawn.runv(command, env=env)


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
            f"unable to connect to postgres:{e}",
            hint="Have you installed and configured PostgreSQL for passwordless authentication?",
        )


if __name__ == "__main__":
    with ui.error_handler("run"):
        main()
