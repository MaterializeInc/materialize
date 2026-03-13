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
    args = parser.parse_args()

    tasks: list[tuple[str, TaskSpec]] = [
        ("rustfmt", _rustfmt_fn(check=args.check)),
        ("buf", _buf_cmd(check=args.check)),
    ]

    if not os.environ.get("MZDEV_NO_PYTHON"):
        tasks += [
            ("black", _black_cmd(check=args.check)),
            ("ruff", _ruff_cmd(check=args.check)),
            ("ruff-dbt", _ruff_dbt_cmd(check=args.check)),
        ]

    return 1 if run_parallel(tasks, spinner_suffix="formatters") else 0


def _rustfmt_fn(*, check: bool):
    """Return a callable that runs cargo metadata + parallel rustfmt."""

    def run() -> tuple[bool, str]:
        ncpus = os.cpu_count() or 8

        result = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version=1"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return False, result.stderr.strip()

        meta = json.loads(result.stdout)
        kinds = {"lib", "bin", "bench", "test", "example", "proc-macro", "custom-build"}
        paths = [
            t["src_path"]
            for pkg in meta["packages"]
            for t in pkg["targets"]
            if kinds & set(t["kind"])
        ]
        if not paths:
            return True, ""

        # Split into batches and run rustfmt in parallel.
        batch_size = math.ceil(len(paths) / ncpus)
        batches = [paths[i : i + batch_size] for i in range(0, len(paths), batch_size)]

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


def _buf_cmd(*, check: bool) -> list[str]:
    if check:
        return ["buf", "format", "src", "--diff", "--exit-code"]
    return ["buf", "format", "src", "-w"]


def _black_cmd(*, check: bool) -> list[str]:
    args = "--check --quiet" if check else "--quiet"
    return [
        "bash",
        "-c",
        f'git ls-files -z -- "*.py" | xargs -0 bin/pyactivate -m black {args}',
    ]


def _ruff_cmd(*, check: bool) -> list[str]:
    fix = "" if check else " --fix"
    return [
        "bash",
        "-c",
        f'git ls-files -z -- "*.py" ":!misc/dbt-materialize" | xargs -0 bin/pyactivate -m ruff{fix}',
    ]


def _ruff_dbt_cmd(*, check: bool) -> list[str]:
    fix = "" if check else " --fix"
    return [
        "bash",
        "-c",
        f'git ls-files -z -- "misc/dbt-materialize/*.py" | xargs -0 bin/pyactivate -m ruff --target-version=py38{fix}',
    ]


if __name__ == "__main__":
    os.chdir(MZ_ROOT)
    exit(main())
