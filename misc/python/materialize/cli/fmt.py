# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""fmt — formats Rust, Python & Protobuf files in parallel."""

import argparse
import json
import math
import os
import subprocess

from materialize import MZ_ROOT
from materialize.parallel_task import TaskSpec, run_parallel


def main() -> int:
    parser = argparse.ArgumentParser(prog="fmt")
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "files",
        nargs="*",
        help="Files to format. If omitted, formats the whole repository.",
    )
    args = parser.parse_args()

    tasks = (
        _file_tasks(args.files, check=args.check)
        if args.files
        else _repo_tasks(check=args.check)
    )

    return 1 if run_parallel(tasks, spinner_suffix="formatters") else 0


def _repo_tasks(*, check: bool) -> list[tuple[str, TaskSpec]]:
    """Build formatter tasks for the whole repository."""
    tasks: list[tuple[str, TaskSpec]] = [
        ("rustfmt", _rustfmt_fn(check=check)),
        ("buf", _buf_cmd(check=check)),
    ]

    if not os.environ.get("MZDEV_NO_PYTHON"):
        tasks += [
            ("black", _black_cmd(check=check)),
            ("ruff", _ruff_cmd(check=check)),
            ("ruff-dbt", _ruff_dbt_cmd(check=check)),
        ]

    return tasks


def _file_tasks(files: list[str], *, check: bool) -> list[tuple[str, TaskSpec]]:
    """Build formatter tasks scoped to an explicit list of files.

    Files are dispatched to formatters by extension. Files with an extension no
    formatter handles are ignored.
    """
    # Normalize to repo-relative paths so the dbt prefix check below works
    # regardless of how the caller spelled the path.
    rel_files = [os.path.relpath(os.path.abspath(f), MZ_ROOT) for f in files]

    rust = [f for f in rel_files if f.endswith(".rs")]
    proto = [f for f in rel_files if f.endswith(".proto")]
    python = [f for f in rel_files if f.endswith(".py") or f.endswith(".pyi")]

    no_python = bool(os.environ.get("MZDEV_NO_PYTHON"))
    dbt = [f for f in python if f.startswith("misc/dbt-materialize/")]
    non_dbt = [f for f in python if not f.startswith("misc/dbt-materialize/")]

    tasks: list[tuple[str, TaskSpec]] = []
    if rust:
        tasks.append(("rustfmt", _rustfmt_fn(check=check, paths=rust)))
    if proto:
        tasks.append(("buf", _buf_cmd(check=check, files=proto)))
    if python and not no_python:
        tasks.append(("black", _black_cmd(check=check, files=python)))
        if non_dbt:
            tasks.append(("ruff", _ruff_cmd(check=check, files=non_dbt)))
        if dbt:
            tasks.append(("ruff-dbt", _ruff_dbt_cmd(check=check, files=dbt)))

    return tasks


def _rustfmt_fn(*, check: bool, paths: list[str] | None = None):
    """Return a callable that runs rustfmt over the given paths.

    When `paths` is None, derives the set of files from cargo metadata and runs
    over the whole workspace.
    """

    def run() -> tuple[bool, str]:
        ncpus = os.cpu_count() or 8

        if paths is None:
            result = subprocess.run(
                ["cargo", "metadata", "--no-deps", "--format-version=1"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                return False, result.stderr.strip()

            meta = json.loads(result.stdout)
            kinds = {
                "lib",
                "bin",
                "bench",
                "test",
                "example",
                "proc-macro",
                "custom-build",
            }
            resolved = [
                t["src_path"]
                for pkg in meta["packages"]
                for t in pkg["targets"]
                if kinds & set(t["kind"])
            ]
        else:
            resolved = paths
        if not resolved:
            return True, ""

        # Split into batches and run rustfmt in parallel.
        batch_size = math.ceil(len(resolved) / ncpus)
        batches = [
            resolved[i : i + batch_size] for i in range(0, len(resolved), batch_size)
        ]

        cmd_base = ["rustfmt", "--config", "error_on_line_overflow=true"]
        if check:
            cmd_base.append("--check")

        procs = [
            subprocess.Popen(
                cmd_base + batch,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            for batch in batches
        ]

        all_output = []
        all_ok = True
        for proc in procs:
            stdout, _ = proc.communicate()
            if proc.returncode != 0:
                all_ok = False
            out = stdout.decode("utf-8").strip()
            if out:
                all_output.append(out)

        return all_ok, "\n".join(all_output)

    return run


def _buf_cmd(*, check: bool, files: list[str] | None = None) -> list[str]:
    # buf format accepts explicit file paths in place of the `src` directory.
    targets = files if files is not None else ["src"]
    if check:
        return ["buf", "format", *targets, "--diff", "--exit-code"]
    return ["buf", "format", *targets, "-w"]


def _black_cmd(*, check: bool, files: list[str] | None = None) -> list[str]:
    args = "--check --quiet" if check else "--quiet"
    if files is not None:
        # With an explicit file list there is no discovery pipeline, so invoke
        # black directly without a shell.
        return ["bin/pyactivate", "-m", "black", *args.split(), *files]
    return [
        "bash",
        "-c",
        f'. misc/shlib/shlib.bash && git_files "*.py" | xargs bin/pyactivate -m black {args}',
    ]


def _ruff_cmd(*, check: bool, files: list[str] | None = None) -> list[str]:
    fix = "" if check else " --fix"
    if files is not None:
        return ["bin/pyactivate", "-m", "ruff", *fix.split(), *files]
    return [
        "bash",
        "-c",
        f'. misc/shlib/shlib.bash && git_files "*.py" | grep -v "^misc/dbt-materialize/" | xargs bin/pyactivate -m ruff{fix}',
    ]


def _ruff_dbt_cmd(*, check: bool, files: list[str] | None = None) -> list[str]:
    fix = "" if check else " --fix"
    if files is not None:
        return [
            "bin/pyactivate",
            "-m",
            "ruff",
            "--target-version=py38",
            *fix.split(),
            *files,
        ]
    return [
        "bash",
        "-c",
        f'. misc/shlib/shlib.bash && git_files "misc/dbt-materialize/*.py" | xargs bin/pyactivate -m ruff --target-version=py38{fix}',
    ]


if __name__ == "__main__":
    os.chdir(MZ_ROOT)
    exit(main())
