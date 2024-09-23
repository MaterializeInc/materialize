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

from materialize import MZ_ROOT, bazel, ui


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="bazel",
        description="Build, run, test, and generate packages with Bazel.",
    )
    parser.add_argument("action", help="Action to run.")

    (args, sub_args) = parser.parse_known_args()

    # Always update our side-channel git hash incase some command needs it.
    bazel.write_git_hash()

    if args.action == "gen":
        gen_cmd(sub_args)
    elif args.action == "fmt":
        fmt_cmd(sub_args)
    elif args.action == "output_path":
        output_path_cmd(sub_args)
    else:
        bazel_cmd([args.action] + sub_args)

    return 0


def gen_cmd(args: list[str]):
    """Invokes the gen function."""

    parser = argparse.ArgumentParser(
        prog="gen", description="Generate BUILD.bazel files."
    )
    parser.add_argument(
        "path",
        type=pathlib.Path,
        help="Path to a Cargo.toml file to generate a BUILD.bazel for.",
        nargs="?",
    )

    gen_args = parser.parse_args(args=args)
    if gen_args.path:
        path = str(os.path.abspath(gen_args.path))
    else:
        path = None

    gen(path)


def fmt_cmd(args: list[str]):
    """Invokes the fmt function."""
    assert len(args) <= 1, "expected at most one path to format"
    path = args[0] if len(args) == 1 else None
    fmt(path)


def output_path_cmd(args: list[str]):
    """Invokes the output_path function."""
    assert len(args) == 1, "expected a single Bazel target"
    target = args[0]
    paths = bazel.output_paths(target)
    for path in paths:
        print(path)


def bazel_cmd(args: list[str]):
    """Forwards all arguments to Bazel."""
    subprocess.run(["bazel"] + args, check=True)


def gen(path):
    """
    Generates BUILD.bazel files from Cargo.toml.

    Defaults to generating for the entire Cargo Workspace, or only a single
    Cargo.toml, if a path is provided.
    """

    if not path:
        path = MZ_ROOT / "Cargo.toml"

    # Note: We build cargo-gazelle with optimizations because the speedup is
    # worth it and Bazel should cache the resulting binary.

    # TODO(parkmycar): Use Bazel to run this lint.
    cmd_args = [
        "cargo",
        "run",
        "--release",
        "--no-default-features",
        "--manifest-path=misc/bazel/cargo-gazelle/Cargo.toml",
        "--",
        "--path",
        f"{str(path)}",
    ]
    subprocess.run(cmd_args, check=True)

    # Make sure we format everything after generating it so the linter doesn't
    # conflict with the formatter.
    fmt(path.parent)


def fmt(path):
    """
    Formats all of the `BUILD`, `.bzl`, and `WORKSPACE` files at the provided path.

    Defaults to formatting the entire Materialize repository.
    """

    if not path:
        path = MZ_ROOT

    if subprocess.run(["which", "bazel"]).returncode != 0:
        ui.warn("couldn't find 'bazel' skipping formatting of BUILD files")
        return

    cmd_args = [
        "bazel",
        "run",
        "//misc/bazel/tools:buildifier",
        "--",
        "-r",
        f"{str(path)}",
    ]
    subprocess.run(cmd_args, check=True)


def output_path(target) -> pathlib.Path:
    """Returns the absolute path of the Bazel target."""

    cmd_args = ["bazel", "cquery", f"{target}", "--output=files"]
    path = subprocess.check_output(cmd_args, text=True)
    return pathlib.Path(path)


if __name__ == "__main__":
    main()
