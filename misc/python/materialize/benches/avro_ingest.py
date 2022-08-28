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
import time
from typing import IO, NamedTuple

import docker
import pg8000
import psutil
import requests
from docker.models.containers import Container
from pg8000.dbapi import ProgrammingError

from materialize import ROOT, mzbuild


def wait_for_confluent(host: str) -> None:
    url = f"http://{host}:8081/subjects"
    while True:
        try:
            print(f"Checking if schema registry at {url} is accessible...")
            r = requests.get(url)
            if r.status_code == 200:
                print("Schema registry is ready")
                return
        except requests.exceptions.ConnectionError as e:
            print(e)
            time.sleep(5)


def mz_proc(container: Container) -> psutil.Process:
    container.reload()
    docker_init = psutil.Process(container.attrs["State"]["Pid"])
    for child in docker_init.children(recursive=True):
        if child.name() == "materialized":
            return child
    raise RuntimeError("Couldn't find materialized pid")


class PrevStats(NamedTuple):
    wall_time: float
    user_cpu: float
    system_cpu: float


def print_stats(container: Container, prev: PrevStats, file: IO) -> PrevStats:
    proc = mz_proc(container)
    memory = proc.memory_info()
    cpu = proc.cpu_times()
    new_prev = PrevStats(time.time(), cpu.user, cpu.system)
    print(
        f"{memory.rss},{memory.vms},{new_prev.user_cpu - prev.user_cpu},{new_prev.system_cpu - prev.system_cpu},{new_prev.wall_time - prev.wall_time}",
        file=file,
        flush=True,
    )
    return new_prev


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--confluent-host",
        default="confluent",
        help="The hostname of a machine running the Confluent Platform",
    )
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
    args = parser.parse_args()

    os.chdir(ROOT)
    repo = mzbuild.Repository(ROOT)

    wait_for_confluent(args.confluent_host)

    images = ["kgen", "materialized"]
    deps = repo.resolve_dependencies([repo.images[name] for name in images])
    deps.acquire()

    docker_client = docker.from_env()

    mz_container = docker_client.containers.run(
        deps["materialized"].spec(),
        detach=True,
        network_mode="host",
    )

    docker_client.containers.run(
        deps["kgen"].spec(),
        [
            f"--num-records={args.records}",
            f"--bootstrap-server={args.confluent_host}:9092",
            f"--schema-registry-url=http://{args.confluent_host}:8081",
            "--topic=bench_data",
            "--keys=avro",
            "--values=avro",
            f"--avro-schema={VALUE_SCHEMA}",
            f"--avro-distribution={VALUE_DISTRIBUTION}",
            f"--avro-key-schema={KEY_SCHEMA}",
            f"--avro-key-distribution={KEY_DISTRIBUTION}",
        ],
        network_mode="host",
    )

    conn = pg8000.connect(host="localhost", port=6875, user="materialize")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"""CREATE CONNECTION IF NOT EXISTS csr_conn
        FOR CONFLUENT SCHEMA REGISTRY
        URL 'http://{args.confluent_host}:8081'"""
    )
    cur.execute(
        f"""CREATE SOURCE src
        FROM KAFKA BROKER '{args.confluent_host}:9092' TOPIC 'bench_data'
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn"""
    )

    results_file = open("results.csv", "w")

    print("Rss,Vms,User Cpu,System Cpu,Wall Time", file=results_file, flush=True)
    prev = PrevStats(time.time(), 0.0, 0.0)
    for _ in range(args.trials):
        cur.execute("DROP VIEW IF EXISTS cnt")
        cur.execute("CREATE MATERIALIZED VIEW cnt AS SELECT count(*) FROM src")
        while True:
            try:
                cur.execute("SELECT * FROM cnt")
                n = cur.fetchone()[0]
                if n >= args.records:
                    break
            except ProgrammingError:
                pass
            time.sleep(1)
        prev = print_stats(mz_container, prev, results_file)


KEY_SCHEMA = json.dumps(
    {
        "name": "testrecordkey",
        "type": "record",
        "namespace": "com.acme.avro",
        "fields": [{"name": "Key1", "type": "long"}, {"name": "Key2", "type": "long"}],
    }
)

KEY_DISTRIBUTION = json.dumps(
    {
        "com.acme.avro.testrecordkey::Key1": [0, 100],
        "com.acme.avro.testrecordkey::Key2": [0, 250000],
    }
)

VALUE_SCHEMA = json.dumps(
    {
        "name": "testrecord",
        "type": "record",
        "namespace": "com.acme.avro",
        "fields": [
            {"name": "Key1Unused", "type": "long"},
            {"name": "Key2Unused", "type": "long"},
            {
                "name": "OuterRecord",
                "type": {
                    "name": "OuterRecord",
                    "type": "record",
                    "fields": [
                        {
                            "name": "Record1",
                            "type": {
                                "name": "Record1",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "InnerRecord1",
                                        "type": {
                                            "name": "InnerRecord1",
                                            "type": "record",
                                            "fields": [
                                                {"name": "Point", "type": "long"}
                                            ],
                                        },
                                    },
                                    {
                                        "name": "InnerRecord2",
                                        "type": {
                                            "name": "InnerRecord2",
                                            "type": "record",
                                            "fields": [
                                                {"name": "Point", "type": "long"}
                                            ],
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            "name": "Record2",
                            "type": {
                                "name": "Record2",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "InnerRecord3",
                                        "type": {
                                            "name": "InnerRecord3",
                                            "type": "record",
                                            "fields": [
                                                {"name": "Point", "type": "long"}
                                            ],
                                        },
                                    },
                                    {
                                        "name": "InnerRecord4",
                                        "type": {
                                            "name": "InnerRecord4",
                                            "type": "record",
                                            "fields": [
                                                {"name": "Point", "type": "long"}
                                            ],
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        ],
    }
)

VALUE_DISTRIBUTION = json.dumps(
    {
        "com.acme.avro.testrecord::Key1Unused": [0, 100],
        "com.acme.avro.testrecord::Key2Unused": [0, 250000],
        "com.acme.avro.InnerRecord1::Point": [10000, 1000000000],
        "com.acme.avro.InnerRecord2::Point": [10000, 1000000000],
        "com.acme.avro.InnerRecord3::Point": [10000, 1000000000],
        "com.acme.avro.InnerRecord4::Point": [10000, 10000000000],
    }
)


if __name__ == "__main__":
    main()
