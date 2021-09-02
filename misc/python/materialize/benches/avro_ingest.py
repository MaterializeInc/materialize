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

import psutil
import psycopg2  # type: ignore
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


def generate_data(n_records: int) -> None:
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


def print_stats(cid: str, wall_time: float) -> None:
    proc = mz_proc(cid)
    memory = proc.memory_info()  # type: ignore
    cpu = proc.cpu_times()  # type: ignore
    print(f"{memory.rss},{memory.vms},{cpu.user},{cpu.system},{wall_time}")


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
    ns = parser.parse_args()

    wait_for_confluent()
    mz_launcher = Thread(target=launch_mz, daemon=True)
    mz_launcher.start()

    kgen_launcher = Thread(target=generate_data, args=[ns.records])
    kgen_launcher.start()
    kgen_launcher.join()

    cid_path = Path("docker.cid")
    cid = ""
    while not cid_path.exists():
        time.sleep(1)
    while not cid:
        with open(cid_path) as f:
            cid = f.read()
    os.remove(cid_path)
    conn = psycopg2.connect("host=localhost port=6875 user=materialize")
    conn.autocommit = True
    cur = conn.cursor()
    print("Rss,Vms,User Cpu,System Cpu,Wall Time")
    cur.execute(
        "CREATE SOURCE s FROM KAFKA BROKER 'confluent:9093' TOPIC 'bench_data' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://confluent:8081'"
    )
    for _ in range(ns.trials):
        start_time = time.time()
        cur.execute("DROP VIEW IF EXISTS ct")
        cur.execute("CREATE MATERIALIZED VIEW ct AS SELECT count(*) FROM s")
        while True:
            try:
                cur.execute("SELECT * FROM ct")
                n = cur.fetchone()[0]
                if n == ns.records:
                    break
            except psycopg2.errors.SqlStatementNotYetComplete:
                pass
            time.sleep(1)
        done_time = time.time()
        print_stats(cid, done_time - start_time)


if __name__ == "__main__":
    main()
