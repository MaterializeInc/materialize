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
import os
import subprocess
import sys
import typing

import psutil  # type: ignore


def rev_parse(git_ref: str) -> str:
    if not git_ref:
        return git_ref
    return subprocess.check_output(["git", "rev-parse", git_ref]).strip().decode()


def main(
    composition: str,
    benchmark: str,
    worker_counts: typing.List[int],
    git_revisions: typing.List[typing.Union[str, None]],
) -> None:

    if benchmark == "benchmark-ci":
        # Explicitly override the worker counts for the CI benchmark
        worker_counts = [1]

    setup_benchmark = [
        "./bin/mzcompose",
        "--mz-find",
        composition,
        "run",
        f"setup-{benchmark}",
    ]
    run_benchmark = [
        "./bin/mzcompose",
        "--mz-find",
        composition,
        "run",
        f"run-{benchmark}",
    ]

    field_names = [
        "git_revision",
        "num_workers",
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

    for (worker_count, git_revision) in itertools.product(worker_counts, git_revisions):

        child_env = os.environ.copy()
        child_env["MZ_WORKERS"] = str(worker_count)
        child_env["MZBUILD_WAIT_FOR_IMAGE"] = "true"
        if git_revision:
            child_env["MZBUILD_MATERIALIZED_TAG"] = f"unstable-{git_revision}"

        try:
            output = subprocess.check_output(
                run_benchmark, env=child_env, stderr=subprocess.STDOUT
            )
        except (subprocess.CalledProcessError,) as e:
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
                "git_revision": git_revision if git_revision else "NONE",
                "num_workers": worker_count,
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
    max_cpus = round(psutil.cpu_count(logical=False) * 0.85)
    num_trials = 4

    # Yield the fractional points (4/4, 3/4, ...) between max and 0, not including 0
    worker_counts = [round(i * max_cpus / num_trials) for i in range(num_trials, 0, -1)]

    return list(reversed(sorted(set(worker_counts))))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--size",
        type=str,
        default="benchmark-medium",
        choices=["benchmark-medium", "benchmark-ci", "benchmark"],
        help="Name of the mzcompose composition to run",
    )

    parser.add_argument(
        "composition", type=str, help="Name of the mzcompose composition to run",
    )

    parser.add_argument(
        "git_references",
        type=str,
        nargs="*",
        help="Materialized builds to test as well, identified by git reference",
    )

    args = parser.parse_args()
    worker_counts = enumerate_cpu_counts()
    git_revisions = [None, *[rev_parse(ref) for ref in args.git_references]]

    main(args.composition, args.size, worker_counts, git_revisions)
