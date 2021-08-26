# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Launch benchmark for a particular commit on cloud infrastructure, using `scratch.py`"""

import argparse
import asyncio
import csv
import itertools
import json
import shlex
import sys
import time
from datetime import timedelta
from typing import List, NamedTuple, Optional, cast

from materialize import git, scratch, util
from materialize.cli.scratch import (
    DEFAULT_INSTPROF_NAME,
    DEFAULT_SG_ID,
    DEFAULT_SUBNET_ID,
    check_required_vars,
)
from materialize.scratch import (
    get_instances_by_tag,
    instance_typedef_tags,
    print_instances,
    run_ssm,
)


# This is duplicated with the one in cli/scratch.
# TODO - factor it out.
def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    for name, configure, run in [
        ("start", configure_start, start),
        ("check", configure_check, check),
        #        ("mine", mine.configure_parser, mine.run),
        #        ("destroy", destroy.configure_parser, destroy.run),
    ]:
        s = subparsers.add_parser(name)
        configure(s)
        s.set_defaults(run=run)

    args = parser.parse_args()
    args.run(args)


def configure_start(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile",
        choices=["basic", "confluent"],
        type=str,
        required=True,
        help="Predefined set of machines to use in the cluster. 'basic' is only the Materialize instance; 'confluent' also includes a machine running the Kafka, Schema Registry, etc.",
    )
    parser.add_argument(
        "--trials",
        "-n",
        type=int,
        default=1,
        help="The number of trials to run per git rev",
    )
    parser.add_argument(
        "--revs",
        type=str,
        default="HEAD",
        help="Comma-separated list of git revs to benchmark",
    )
    parser.add_argument(
        "bench_script",
        type=str,
        nargs=argparse.REMAINDER,
        help="Benchmark script (and optional arguments)",
    )


class BenchResult(NamedTuple):
    exit_code: int
    stdout: str
    stderr: str


def configure_check(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("bench_id", type=str, nargs=1)


def check(ns: argparse.Namespace) -> None:
    check_required_vars()
    bench_id = ns.bench_id[0]
    insts = get_instances_by_tag("bench_id", bench_id)
    if not insts:
        raise RuntimeError(f"No instances found for bench ID {bench_id}")
    results: List[Optional[BenchResult]] = [None for _ in insts]
    not_done = list(range(len(results)))
    script = """#!/usr/bin/env bash
    if [[ -f /home/ubuntu/bench_exit_code ]]; then
        jq -aRn "{\\"stdout\\": \\$out, \\"stderr\\": \\$err, \\"exit_code\\": $(cat /home/ubuntu/bench_exit_code)}" --rawfile out /home/ubuntu/mzscratch-startup.out --rawfile err /home/ubuntu/mzscratch-startup.err
    else
        echo '"NOT DONE"'
    fi"""
    while not_done:
        futures = [run_ssm(insts[i]["InstanceId"], [script]) for i in not_done]
        loop = asyncio.get_event_loop()
        new_results = loop.run_until_complete(asyncio.gather(*futures))
        for result, i in zip(new_results, not_done):
            json_result = json.loads(result.stdout)
            if json_result != "NOT DONE":
                results[i] = BenchResult(
                    json_result["exit_code"],
                    json_result["stdout"],
                    json_result["stderr"],
                )
        not_done = [i for i in not_done if not results[i]]
        if not_done:
            print("Benchmark not done; waiting 60 seconds", file=sys.stderr)
            time.sleep(60)
    for r in results:
        assert isinstance(r, BenchResult)
    results = cast(List[BenchResult], results)
    failed = [r for r in results if r.exit_code]
    if failed:
        # TODO - something better here. Which hosts failed? Etc.
        raise RuntimeError(
            f"{len(failed)} runs FAILED! Check mzscratch-startup.err on the hosts for more details"
        )
    readers = [
        csv.DictReader(f"{line}\n" for line in r.stdout.split("\n")) for r in results
    ]
    csv_results = ((d.values() for d in r) for r in readers)
    for r in readers:
        assert isinstance(r.fieldnames, list)
        for fn in r.fieldnames:
            assert isinstance(fn, str)
    headers = set(tuple(cast(List[str], r.fieldnames)) for r in readers)
    if len(headers) > 1:
        raise RuntimeError("Mismatched headers")
    w = csv.writer(sys.stdout)
    w.writerow(
        cast(List[str], readers[0].fieldnames) + ["InstanceIndex", "Rev", "Trial"]
    )
    for inst, r in zip(insts, csv_results):
        tags = instance_typedef_tags(inst)
        for i, entry in enumerate(r):
            w.writerow(itertools.chain(entry, (tags["bench_i"], tags["bench_rev"], i)))


def start(ns: argparse.Namespace) -> None:
    check_required_vars()

    revs = ns.revs.split(",")

    clusters = itertools.product(range(ns.trials), (git.rev_parse(rev) for rev in revs))

    if ns.profile != "basic":
        raise RuntimeError(f"Profile {ns.profile} is not implemented yet")

    bench_script = ns.bench_script
    script_name = bench_script[0]
    script_args = " ".join((shlex.quote(arg) for arg in bench_script[1:]))
    script_is_py = script_name.endswith(".py")

    with open(script_name) as f:
        script_text = f.read()

    if script_is_py:
        launch_script = f"""echo {shlex.quote(script_text)} > misc/python/materialize/cloudbench_script.py
bin/pyactivate --dev -u -m materialize.cloudbench_script {script_args}
echo $? > ~/bench_exit_code
"""
    else:
        assert False  # TODO

    bench_id = util.nonce(8)
    # TODO - Do these in parallel
    launched = []
    for (i, rev) in clusters:
        desc = scratch.MachineDesc(
            name="materialized",
            launch_script=launch_script,
            instance_type="r5ad.4xlarge",
            ami="ami-0b29b6e62f2343b46",
            tags={},
            size_gb=64,
        )
        launched += scratch.launch_cluster(
            descs=[desc],
            nonce=f"{bench_id}-{i}-{rev}",
            subnet_id=DEFAULT_SUBNET_ID,
            security_group_id=DEFAULT_SG_ID,
            instance_profile=DEFAULT_INSTPROF_NAME,
            key_name=None,
            extra_tags={
                "bench_id": bench_id,
                "bench_rev": rev,
                "bench_i": str(i),
                "LaunchedBy": scratch.whoami(),
            },
            delete_after=scratch.now_plus(timedelta(days=1)),
            git_rev=rev,
        )

    print("Launched instances:")
    print_instances(launched, format="table")  # todo
    print(
        f"""Launched cloud bench with ID {bench_id}.
To wait for results, run: bin/cloudbench check {bench_id}"""
    )


if __name__ == "__main__":
    main()
