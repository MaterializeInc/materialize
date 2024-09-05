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

from materialize import MZ_ROOT, ui
from materialize.bazel.utils import output_paths as bazel_output_paths

# Path where we put the current revision of the repo that we can side channel
# into Bazel.
MZ_GIT_HASH_FILE = "/tmp/mz_git_hash.txt"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="bazel",
        description="Build, run, test, and generate packages with Bazel.",
    )
    parser.add_argument("action", help="Action to run.")

    (args, sub_args) = parser.parse_known_args()

    # Always update our side-channel git hash incase some command needs it.
    write_git_hash()

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
    paths = bazel_output_paths(target)
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
        "-c",
        "opt",
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


def write_git_hash():
    """
    Temporary file where we write the current git hash, so we can side channel
    it into Bazel.

    For production releases we stamp builds with the `workspace_status_command`
    but this workflow is not friendly to remote caching. Specifically, the
    "volatile status" of a workspace is not supposed to cause builds to get
    invalidated, and it doesn't when the result is cached locally, but it does
    when it's cached remotely.

    See: <https://bazel.build/docs/user-manual#workspace-status>
         <https://github.com/bazelbuild/bazel/issues/10075>
    """

    repo = MZ_ROOT / ".git"
    cmd_args = ["git", f"--git-dir={repo}", "rev-parse", "HEAD"]
    result = subprocess.run(
        cmd_args, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )

    if result.returncode == 0:
        with open(MZ_GIT_HASH_FILE, "w") as f:
            f.write(result.stdout.strip())
    else:
        ui.warn(f"Failed to get current revision of {MZ_ROOT}, falling back to all 0s")


if __name__ == "__main__":
    main()
