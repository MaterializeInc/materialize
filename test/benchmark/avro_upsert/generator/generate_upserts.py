#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import pathlib
import subprocess

SHARED_FILES = "/usr/share/generator"


def run(args: argparse.Namespace) -> None:
    """Run the generator, inserting args.num_messages number of messages."""

    messages_per_process = int(args.num_messages / args.parallelism)

    value_schema = pathlib.Path(SHARED_FILES, "value-schema.json").read_text().strip()
    value_distribution = (
        pathlib.Path(SHARED_FILES, "value-distribution.json").read_text().strip()
    )

    kafka_gen = [
        "/usr/local/bin/kgen",
        "--quiet",
        "--bootstrap-server",
        "kafka:9092",
        "--schema-registry",
        "http://schema-registry:8081",
        "--num-messages",
        str(messages_per_process),
        "--topic",
        "upsertavrotest",
        "--partitions",
        "30",
        "--keys",
        "random",
        "--key-min",
        "0",
        "--key-max",
        str(args.num_keys),
        "--values",
        "avro",
        "--avro-schema",
        value_schema,
        "--avro-distribution",
        value_distribution,
    ]

    print(
        f"Spawning {args.parallelism} generator processes, writing {messages_per_process} messages each"
    )
    procs = [subprocess.Popen(kafka_gen) for _ in range(0, args.parallelism)]
    for (i, p) in enumerate(procs):
        p.wait()
        print(
            f"{i}/{args.parallelism} processes finished: pid={p.pid} returncode={p.returncode}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-k",
        "--num-keys",
        type=int,
        default=25000000,
        help="Number of unique keys to generate (should match value-distribution!)",
    )
    parser.add_argument(
        "-n",
        "--num-messages",
        type=int,
        default=400000000,
        help="Total number of messages to generate",
    )
    parser.add_argument(
        "-p", "--parallelism", type=int, default=40, help="Number of processes to spawn"
    )

    args = parser.parse_args()
    run(args)
