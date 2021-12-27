# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Ingest some Avro records, and report how long it takes"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from threading import Thread
from typing import NamedTuple

import psutil
import psycopg2
import requests

from materialize import mzbuild, spawn

MZ_ROOT = os.environ["MZ_ROOT"]


def wait_for_confluent() -> None:
    while True:
        try:
            r = requests.get("http://confluent:8081/subjects")
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            continue


def generate_data(n_records: int, distribution: str) -> None:
    repo = mzbuild.Repository(Path(MZ_ROOT))
    deps = repo.resolve_dependencies([repo.images["kafka-avro-generator"]])
    image = deps["kafka-avro-generator"]
    deps.acquire()

    stdout = open("gen_data.out", "a")
    stderr = open("gen_data.err", "a")

    spawn.runv(
        args=[
            "docker",
            "run",
            "--network",
            "host",
            image.spec(),
            "-n",
            str(n_records),
            "-b",
            "confluent:9093",
            "-r",
            "http://confluent:8081",
            "-t",
            "bench_data",
            "-d",
            distribution,
        ],
        stdout=stdout,
        stderr=stderr,
        print_to=sys.stderr,
    )


def launch_mz() -> None:
    # We can't run bin/mzimage directly,
    # because we need to pass in various flags;
    # notably `--cidfile` to get the container ID.
    # Otherwise, this all does the same thing as `bin/mzimage materialized`
    repo = mzbuild.Repository(Path(MZ_ROOT))
    deps = repo.resolve_dependencies([repo.images["materialized"]])
    image = deps["materialized"]
    deps.acquire()

    stdout = open("build_mz.out", "a")
    stderr = open("build_mz.err", "a")

    spawn.runv(
        args=[
            "docker",
            "run",
            "--network",
            "host",
            "--cidfile",
            "docker.cid",
            image.spec(),
        ],
        stdout=stdout,
        stderr=stderr,
        print_to=sys.stderr,
    )


def mz_proc(cid: str) -> psutil.Process:
    docker_info = spawn.capture(["docker", "inspect", cid])
    docker_info = json.loads(docker_info)
    docker_init_pid = int(docker_info[0]["State"]["Pid"])
    docker_init = psutil.Process(docker_init_pid)
    for child in docker_init.children(recursive=True):
        if child.name() == "materialized":
            assert isinstance(child, psutil.Process)
            return child
    raise RuntimeError("Couldn't find materialized pid")


class PrevStats(NamedTuple):
    wall_time: float
    user_cpu: float
    system_cpu: float


def print_stats(cid: str, prev: PrevStats) -> PrevStats:
    proc = mz_proc(cid)
    memory = proc.memory_info()
    cpu = proc.cpu_times()
    new_prev = PrevStats(time.time(), cpu.user, cpu.system)
    print(
        f"{memory.rss},{memory.vms},{new_prev.user_cpu - prev.user_cpu},{new_prev.system_cpu - prev.system_cpu},{new_prev.wall_time - prev.wall_time}"
    )
    return new_prev


def main() -> None:
    os.chdir(MZ_ROOT)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--trials",
        default=1,
        type=int,
        help="Number of measurements to take",
    )
    parser.add_argument(
        "-r",
        "--records",
        default=1000000,
        type=int,
        help="Number of Avro records to generate",
    )
    parser.add_argument(
        "-d",
        "--distribution",
        default="benchmark",
        type=str,
        help="Distribution to use in kafka-avro-generator",
    )
    ns = parser.parse_args()

    wait_for_confluent()
    # We need to temporarily redirect stdout to stderr,
    # because, although recent versions of the mz repository
    # make sure mzbuild only writes to stderr here,
    # we might want to run this against older versions that don't.
    #
    # This will not work correctly
    # if stdout is buffered, but we invoke this script
    # with python3 -u, so that's fine.
    #
    # We have to do this the POSIX way here, rather than with
    # `contextlib.redirect_stdout`, because that only affects native
    # Python code, not e.g. spawned processes
    old_stdout = os.dup(1)
    os.dup2(2, 1)

    mz_launcher = Thread(target=launch_mz, daemon=True)
    mz_launcher.start()

    kgen_launcher = Thread(target=generate_data, args=[ns.records, ns.distribution])
    kgen_launcher.start()
    kgen_launcher.join()

    cid_path = Path("docker.cid")
    cid = ""
    while not cid_path.exists():
        time.sleep(1)
    while not cid:
        with open(cid_path) as f:
            cid = f.read()
    os.dup2(old_stdout, 1)
    os.remove(cid_path)
    conn = psycopg2.connect("host=localhost port=6875 user=materialize")
    conn.autocommit = True
    cur = conn.cursor()
    print("Rss,Vms,User Cpu,System Cpu,Wall Time")
    cur.execute(  # type: ignore
        "CREATE SOURCE s FROM KAFKA BROKER 'confluent:9093' TOPIC 'bench_data' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://confluent:8081'"
    )
    prev = PrevStats(time.time(), 0.0, 0.0)
    for _ in range(ns.trials):
        cur.execute("DROP VIEW IF EXISTS ct")  # type: ignore
        cur.execute("CREATE MATERIALIZED VIEW ct AS SELECT count(*) FROM s")  # type: ignore
        while True:
            try:
                cur.execute("SELECT * FROM ct")  # type: ignore
                n = cur.fetchone()[0]
                if n == ns.records:
                    break
            except (
                psycopg2.errors.SqlStatementNotYetComplete,
                psycopg2.errors.InternalError,
            ):
                pass
            time.sleep(1)
        prev = print_stats(cid, prev)


if __name__ == "__main__":
    main()
