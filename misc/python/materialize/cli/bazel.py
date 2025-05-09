# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# bazel.py — build and test with bazel

import argparse
import os
import pathlib
import subprocess
from pathlib import Path

from materialize import MZ_ROOT, bazel, ui
from materialize.bazel import remote_cache_arg
from materialize.build_config import BuildConfig


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="bazel",
        description="Build, run, test, and generate packages with Bazel.",
    )
    parser.add_argument("action", help="Action to run.")

    (args, sub_args) = parser.parse_known_args()
    config = BuildConfig.read()

    # Always update our side-channel git hash incase some command needs it.
    bazel.write_git_hash()

    if args.action == "gen":
        gen_cmd(config, sub_args)
    elif args.action == "fmt":
        fmt_cmd(config, sub_args)
    elif args.action == "output_path":
        output_path_cmd(config, sub_args)
    elif args.action == "check":
        check_cmd(config, sub_args)
    elif args.action == "integrity":
        integrity_cmd(config, sub_args)
    else:
        bazel_cmd(config, [args.action] + sub_args)

    return 0


def check_cmd(config: BuildConfig, args: list[str]):
    """
    Invokes a `bazel build` with `cargo check` like behavior.

    Still experimental, is known to fail with crates that have pipelined compilation explicitly
    disabled.
    """
    check_args = ["build", "--config=check", *args]
    bazel_cmd(config, check_args)


def gen_cmd(config: BuildConfig, args: list[str]):
    """Invokes the gen function."""

    parser = argparse.ArgumentParser(
        prog="gen", description="Generate BUILD.bazel files."
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "path",
        type=pathlib.Path,
        help="Path to a Cargo.toml file to generate a BUILD.bazel for.",
        nargs="?",
    )

    gen_args = parser.parse_args(args=args)
    if gen_args.path:
        path = Path(os.path.abspath(gen_args.path))
    else:
        path = None

    gen(config, path, gen_args.check)


def fmt_cmd(config: BuildConfig, args: list[str]):
    """Invokes the fmt function."""
    assert len(args) <= 1, "expected at most one path to format"
    path = args[0] if len(args) == 1 else None
    fmt(config, path)


def output_path_cmd(config: BuildConfig, args: list[str]):
    """Invokes the output_path function."""
    assert len(args) == 1, "expected a single Bazel target"
    target = args[0]
    paths = bazel.output_paths(target)
    for path in paths:
        print(path)


def integrity_cmd(config: BuildConfig, args: list[str]):
    """Calculate the integrity value for a file."""
    if args[0] == "toolchains":
        assert len(args) == 3, "expected <stable version> <nightly version>"
        stable = args[1]
        nightly = args[2]
        hashes = bazel.toolchain_hashes(stable, nightly)
        print(hashes)
    else:
        for path in args:
            integrity = bazel.calc_ingerity(path)
            print(integrity)


def bazel_cmd(config: BuildConfig, args: list[str]):
    """Forwards all arguments to Bazel, possibly with extra configuration."""
    remote_cache = remote_cache_arg(config)
    try:
        subprocess.run(["bazel", *args, *remote_cache], check=True)
    except:
        # Don't print any python backtrace because it's never useful. Instead
        # just exit the process.
        exit(1)


def gen(config: BuildConfig, path: Path | None, check: bool):
    """
    Generates BUILD.bazel files from Cargo.toml.

    Defaults to generating for the entire Cargo Workspace, or only a single
    Cargo.toml, if a path is provided.
    """

    if not path:
        path = MZ_ROOT / "Cargo.toml"

    check_arg = []
    if check:
        check_arg += ["--check"]
    remote_cache = remote_cache_arg(config)

    cmd_args = [
        "bazel",
        "run",
        *remote_cache,
        # TODO(parkmycar): Once bin/bazel gen is more stable in CI, enable this
        # config to make the output less noisy.
        # "--config=script",
        "//misc/bazel/tools:cargo-gazelle",
        "--",
        *check_arg,
        f"{str(path)}",
    ]
    subprocess.run(cmd_args, check=True)


def fmt(config: BuildConfig, path):
    """
    Formats all of the `BUILD`, `.bzl`, and `WORKSPACE` files at the provided path.

    Defaults to formatting the entire Materialize repository.
    """

    if not path:
        path = MZ_ROOT

    if subprocess.run(["which", "bazel"]).returncode != 0:
        ui.warn("couldn't find 'bazel' skipping formatting of BUILD files")
        return

    # Note: No remote cache is needed here since we're just running an already
    # built binary.
    cmd_args = [
        "bazel",
        "run",
        "--config=script",
        "//misc/bazel/tools:buildifier",
        "--",
        "-r",
        f"{str(path)}",
    ]
    subprocess.run(cmd_args, check=True)


def output_path(target) -> list[pathlib.Path]:
    """Returns the absolute path of the Bazel target."""

    cmd_args = ["bazel", "cquery", f"{target}", "--output=files"]
    paths = subprocess.check_output(
        cmd_args, text=True, stderr=subprocess.DEVNULL
    ).splitlines()
    return [pathlib.Path(path) for path in paths]


if __name__ == "__main__":
    main()
