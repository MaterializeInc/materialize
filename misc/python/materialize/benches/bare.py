# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run Materialize for a specified number of seconds, then report system metrics"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from threading import Thread

import psutil

from materialize import mzbuild, spawn

MZ_ROOT = os.environ["MZ_ROOT"]


def launch_mz() -> None:
    # We can't run bin/mzimage directly,
    # because we need to pass in `--cidfile`
    # to get the container ID. Otherwise,
    # this all does the same thing as `bin/mzimage materialized`
    repo = mzbuild.Repository(Path(MZ_ROOT))
    deps = repo.resolve_dependencies([repo.images["materialized"]])
    image = deps["materialized"]
    deps.acquire()

    stdout = open("bench.out", "a")
    stderr = open("bench.err", "a")

    spawn.runv(
        args=["docker", "run", "--cidfile", "docker.cid", image.spec()],
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


def print_stats(cid: str) -> None:
    proc = mz_proc(cid)
    memory = proc.memory_info()  # type: ignore
    cpu = proc.cpu_times()  # type: ignore
    print(f"{memory.rss},{memory.vms},{cpu.user},{cpu.system}")


def main() -> None:
    os.chdir(MZ_ROOT)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--time",
        required=True,
        type=int,
        help="Delay between measurements",
    )
    parser.add_argument(
        "-n",
        "--trials",
        default=1,
        type=int,
        help="Number of measurements to take",
    )
    ns = parser.parse_args()

    mz_launcher = Thread(target=launch_mz, daemon=True)
    mz_launcher.start()

    cid_path = Path("docker.cid")
    cid = ""
    while not cid_path.exists():
        time.sleep(1)
    while not cid:
        with open(cid_path) as f:
            cid = f.read()
    os.remove(cid_path)
    print("Rss,Vms,User Cpu,System Cpu")
    for _ in range(ns.trials):
        time.sleep(ns.time)
        print_stats(cid)


if __name__ == "__main__":
    main()
