# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzbuild.py -- script to run materialized benchmarks

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
import webbrowser


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

    if not args.no_cleanup:
        cleanup = [
            mzcompose_location(mz_root),
            "--mz-find",
            args.composition,
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

    if args.no_benchmark_this_checkout:
        git_references = args.git_references
    else:
        git_references = [None, *args.git_references]

    if args.verbose:
        build_tags = [None, *[mzbuild_tag(ref) for ref in args.git_references]]
        print(f"DEBUG: num_iterations={args.num_measurements}")
        print(f"DEBUG: worker_counts={worker_counts}")
        print(f"DEBUG: mzbuild_tags={build_tags}")

    if args.size == "benchmark-ci":
        # Explicitly override the worker counts for the CI benchmark
        worker_counts = [1]

    setup_benchmark = [
        mzcompose_location(mz_root),
        "--mz-find",
        args.composition,
        "run",
        f"setup-benchmark-{args.size}",
    ]
    run_benchmark = [
        mzcompose_location(mz_root),
        "--mz-find",
        args.composition,
        "run",
        f"run-benchmark-{args.size}",
    ]

    field_names = [
        "git_revision",
        "num_workers",
        "iteration",
        "seconds_taken",
        "rows_per_second",
        "grafana_url",
    ]
    results_writer = csv.DictWriter(sys.stdout, field_names)
    results_writer.writeheader()

    # We use check_output because check_call does not capture output
    try:
        subprocess.check_output(setup_benchmark, stderr=subprocess.STDOUT)
    except (subprocess.CalledProcessError,) as e:
        print(
            f"Setup benchmark failed! Output from failed command:\n{e.output.decode()}"
        )
        raise

    if args.web:
        try:
            web_command = [
                mzcompose_location(mz_root),
                "--mz-find",
                args.composition,
                "web",
                f"perf-dash-web",
            ]
            output = subprocess.check_output(web_command, stderr=subprocess.STDOUT)
        except (subprocess.CalledProcessError,) as e:
            print(f"Failed to open browser to perf-dash:\n{e.output.decode()}")
            raise

    iterations = range(0, args.num_measurements)
    for (iteration, worker_count, git_ref) in itertools.product(
        iterations, worker_counts, git_references
    ):

        # Sadly, environment variables are the only way to pass this information into containers
        # started by mzcompose
        child_env = os.environ.copy()
        child_env["MZ_ROOT"] = mz_root
        child_env["MZ_WORKERS"] = str(worker_count)
        child_env["MZBENCH_ID"] = args.benchmark_id
        child_env["MZBUILD_WAIT_FOR_IMAGE"] = "true"
        if git_ref:
            child_env["MZBENCH_GIT_REF"] = git_ref
            child_env["MZBUILD_MATERIALIZED_TAG"] = mzbuild_tag(git_ref)

        try:
            output = subprocess.check_output(
                run_benchmark, env=child_env, stderr=subprocess.STDOUT
            )
        except (subprocess.CalledProcessError,) as e:
            # TODO: Don't exit with error on simple benchmark failure
            print(
                f"Setup benchmark failed! Output from failed command:\n{e.output.decode()}"
            )
            raise

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

        results_writer.writerow(
            {
                "git_revision": git_ref if git_ref else "None",
                "num_workers": worker_count,
                "iteration": iteration,
                "seconds_taken": seconds_taken,
                "rows_per_second": rows_per_second,
                "grafana_url": grafana_url,
            }
        )


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
        help="Skip running `mzcompose down -v` and leave state from previous runs",
    )

    parser.add_argument(
        "--no-benchmark-this-checkout",
        action="store_true",
        help="Don't benchmark the version of materialized in this checkout",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging output"
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

    args = parser.parse_args()
    main(args)
