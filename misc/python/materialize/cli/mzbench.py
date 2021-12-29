# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzbench.py -- script to run materialized benchmarks

import argparse
import csv
import itertools
import multiprocessing
import os
import pathlib
import subprocess
import sys
import typing
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union


class Bench:
    """Abstract base class implemented by mzbench benchmarks."""

    def metrics(self) -> List[Tuple[str, bool]]:
        """Returns the set of metric names, each with
        a boolean determining whether the metric is required to exist.
        """
        raise NotImplementedError

    def run(
        self, git_revision: str, num_workers: int, mzbench_id: str
    ) -> Dict[str, Any]:
        """Runs the benchmark once, and returns a set of metrics.
        The set of metrics and their types must be the same for each run,
        and must match the list returned by `metrics`."""
        raise NotImplementedError

    def teardown(self) -> None:
        """Performs any necessary per-benchmark cleanup.
        Runs once per benchmark, not once per run!"""
        raise NotImplementedError


ArgList = List[Union[str, pathlib.Path]]


class SuccessOutputBench(Bench):
    """Benchmarks that run `mzcompose` and parse its output looking for various fields.
    In order to be usable with this class, the benchmark must emit a line of the form
    `SUCCESS! seconds_taken=<x> rows_per_second=<y>`.

    Optionally, the benchmark may report Grafana URLs by also emitting a line of the form
    `Grafana URL: <url>`."""

    def __init__(
        self,
        composition: str,
        mz_root: str,
        run_workflow: str,
        setup_workflow: str,
        cleanup: bool,
    ) -> None:
        self.composition = composition
        self.mz_root = mz_root
        self.run_workflow = run_workflow
        self.root = mzcompose_location(self.mz_root)
        if cleanup:
            self.teardown()
        self.run_benchmark: ArgList = [
            self.root,
            "--find",
            self.composition,
            "run",
            self.run_workflow,
        ]
        setup_benchmark: ArgList = [
            self.root,
            "--find",
            self.composition,
            "run",
            setup_workflow,
        ]
        # We use check_output because check_call does not capture output
        try:
            subprocess.check_output(setup_benchmark, stderr=subprocess.STDOUT)
        except (subprocess.CalledProcessError,) as e:
            print(
                f"Setup benchmark failed! Output from failed command:\n{e.output.decode()}"
            )
            raise

    def metrics(self) -> List[Tuple[str, bool]]:
        return [
            ("seconds_taken", True),
            ("rows_per_second", True),
            ("grafana_url", False),
        ]

    def run(
        self, git_revision: Optional[str], num_workers: int, mzbench_id: str
    ) -> Dict[str, Any]:
        # Sadly, environment variables are the only way to pass this information into containers
        # started by mzcompose
        child_env = os.environ.copy()
        child_env["MZ_ROOT"] = self.mz_root
        child_env["MZ_WORKERS"] = str(num_workers)
        child_env["MZBENCH_ID"] = mzbench_id
        child_env["MZBUILD_WAIT_FOR_IMAGE"] = "true"
        if git_revision:
            child_env["MZBENCH_GIT_REF"] = git_revision
            child_env["MZBUILD_MATERIALIZED_TAG"] = mzbuild_tag(git_revision)

        try:
            output = subprocess.check_output(
                self.run_benchmark, env=child_env, stderr=subprocess.STDOUT
            )
        except (subprocess.CalledProcessError,) as e:
            # TODO: Don't exit with error on simple benchmark failure
            print(
                f"Setup benchmark failed! Output from failed command:\n{e.output.decode()}"
            )
            raise

        seconds_taken = None
        rows_per_second = None
        grafana_url = None

        # TODO: Replace parsing output from mzcompose with reading from a well known file or topic
        for line in output.decode().splitlines():
            if line.startswith("SUCCESS!"):
                for token in line.split(" "):
                    if token.startswith("seconds_taken="):
                        seconds_taken = token[len("seconds_taken=") :]
                    elif token.startswith("rows_per_sec="):
                        rows_per_second = token[len("rows_per_sec=") :]
            elif line.startswith("Grafana URL: "):
                grafana_url = line[len("Grafana URL: ") :]

        return {
            "seconds_taken": seconds_taken,
            "rows_per_second": rows_per_second,
            "grafana_url": grafana_url,
        }

    def teardown(self) -> None:
        cleanup: ArgList = [
            self.root,
            "--find",
            self.composition,
            "down",
            "-v",
        ]
        try:
            subprocess.check_output(cleanup, stderr=subprocess.STDOUT)
        except (subprocess.CalledProcessError,) as e:
            print(
                f"Failed to cleanup prior state! Output from failed command:\n{e.output.decode()}"
            )
            raise


from materialize import ui

dbg = ui.speaker("DEBUG: ")


def mzbuild_tag(git_ref: str) -> str:
    if not git_ref:
        return git_ref
    try:
        return (
            subprocess.check_output(
                ["git", "describe", "--exact-match", git_ref], stderr=subprocess.STDOUT
            )
            .strip()
            .decode()
        )
    except subprocess.CalledProcessError:
        unstable_ref = (
            subprocess.check_output(["git", "rev-parse", "--verify", git_ref])
            .strip()
            .decode()
        )
        return f"unstable-{unstable_ref}"


def mzcompose_location(mz_root: str) -> pathlib.Path:
    """Return the absolute path to mzcompose.

    MZ_ROOT is expected to be set via pyactivate.
    """
    return pathlib.Path(mz_root, "bin", "mzcompose")


def main(args: argparse.Namespace) -> None:
    # Ensure that we are working out of the git directory so that commands, such as git, will work
    mz_root = os.environ["MZ_ROOT"]
    os.chdir(mz_root)

    worker_counts = enumerate_cpu_counts()

    if args.no_benchmark_this_checkout:
        git_references = args.git_references
    else:
        git_references = [None, *args.git_references]

    ui.Verbosity.init_from_env(explicit=args.quiet)
    build_tags = [None, *[mzbuild_tag(ref) for ref in args.git_references]]
    dbg(f"num_iterators={args.num_measurements}")
    dbg(f"worker_counts={worker_counts}")
    dbg(f"mzbuild_tags={build_tags}")

    if args.size == "benchmark-ci":
        # Explicitly override the worker counts for the CI benchmark
        worker_counts = [1]

    bench = SuccessOutputBench(
        args.composition,
        mz_root,
        f"run-benchmark-{args.size}",
        f"setup-benchmark-{args.size}",
        not args.no_cleanup,
    )

    metadata_field_names = [
        "git_revision",
        "num_workers",
        "iteration",
    ]

    metrics = {k: v for (k, v) in bench.metrics()}
    results_writer = csv.DictWriter(
        sys.stdout,
        metadata_field_names + [metric for (metric, _optional) in metrics.items()],
    )
    results_writer.writeheader()

    if args.web:
        try:
            web_command = [
                mzcompose_location(mz_root),
                "--find",
                args.composition,
                "web",
                f"perf-dash-web",
            ]
            subprocess.check_output(web_command, stderr=subprocess.STDOUT)
        except (subprocess.CalledProcessError,) as e:
            print(f"Failed to open browser to perf-dash:\n{e.output.decode()}")
            raise

    iterations = range(0, args.num_measurements)
    for (iteration, worker_count, git_ref) in itertools.product(
        iterations, worker_counts, git_references
    ):
        results = bench.run(git_ref, worker_count, args.benchmark_id)
        for (metric, optional) in metrics.items():
            if (not optional) and (results.get(metric) is None):
                print(f"Required metric {metric} not found", metric)
                raise ValueError

        results["git_revision"] = git_ref if git_ref else "None"
        results["num_workers"] = worker_count
        results["iteration"] = iteration

        results_writer.writerow(
            {
                k: v
                for (k, v) in results.items()
                if (k in metadata_field_names or k in metrics)
            }
        )

    if not args.no_teardown:
        bench.teardown()


def enumerate_cpu_counts() -> typing.List[int]:
    """This program prints the number of CPU counts to benchmark on this machine.

    We remove some percentage of CPU cores off the top for system / background processing. With
    the CPUs that remain, we generate a list of evenly spaced worker counts. The list is limited
    by the number of trials desired. This is meant to help us explore the number of CPUs that
    should be dedicated to MZ_WORKERS, not as a prescription for the correct values to choose.

    On a Macbook with 8 cores, this will return [6, 4, 3, 2].

    On a 56 core machine, this returns [24, 18, 12, 6].

    On a 96 core machine, this returns [41, 30, 20, 10].
    """

    # 15% overhead and count physical cores only
    max_cpus = round(multiprocessing.cpu_count() * 0.425)
    num_trials = 4

    # Yield the fractional points (4/4, 3/4, ...) between max and 0, not including 0
    worker_counts = [round(i * max_cpus / num_trials) for i in range(num_trials, 0, -1)]

    return list(reversed(sorted(set(worker_counts))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-b",
        "--benchmark-id",
        type=str,
        default=str(uuid.uuid4()),
        help="Pseudo-unique identifier to use for this benchmark",
    )

    parser.add_argument(
        "-n",
        "--num-measurements",
        type=int,
        default=6,
        help="Number of times to repeat each benchmark iteration",
    )

    parser.add_argument(
        "-s",
        "--size",
        type=str,
        default="medium",
        choices=["medium", "ci", "large"],
        help="Name of the mzcompose composition to run",
    )

    # Ideally we would make this an argparse.BooleanOptionalAction and invert the name to be
    # "--cleanup" but that requires Python3.9
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip running `mzcompose down -v` at the beginning and leave state from previous runs",
    )

    parser.add_argument(
        "--no-benchmark-this-checkout",
        action="store_true",
        help="Don't benchmark the version of materialized in this checkout",
    )

    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Disable verbose logging output"
    )

    parser.add_argument(
        "-w",
        "--web",
        action="store_true",
        help="Open a web browser showing results visualizations",
    )

    parser.add_argument(
        "composition",
        type=str,
        help="Name of the mzcompose composition to run",
    )

    parser.add_argument(
        "git_references",
        type=str,
        nargs="*",
        help="Materialized builds to test as well, identified by git reference",
    )

    parser.add_argument(
        "--no-teardown",
        action="store_true",
        help="Don't tear down benchmark state before exiting",
    )

    args = parser.parse_args()
    main(args)
