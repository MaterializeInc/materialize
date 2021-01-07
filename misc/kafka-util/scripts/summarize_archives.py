#!/usr/bin/env python3
#
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Script to read all topic archives and produce a basic statistical summary of the messages
# contained within

import argparse
import collections
import functools
import io
import json
import logging
import multiprocessing
import glob
import os
import sys
import typing

import avro.io  # type: ignore
import avro.schema  # type: ignore
import pyarrow  # type: ignore
import pyarrow.fs  # type: ignore

# Setup basic formatting for logging output
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO
)
log = logging.getLogger("summarize.archive")


def decode(reader: avro.io.DatumReader, msg: bytes) -> typing.Any:
    """Return the Python object represented by the avro-encoded message."""
    decoder = avro.io.BinaryDecoder(io.BytesIO(msg))
    return reader.read(decoder)


def summarize_archive(archive: str) -> None:
    """Print interesting statistics about an archived Kafka topic."""

    with open("schemas.json") as fd:
        schemas = json.load(fd)

    topic = os.path.splitext(archive)[0]
    try:
        key_schema = schemas[f"{topic}-key"]["schema"]
        value_schema = schemas[f"{topic}-value"]["schema"]
    except KeyError as e:
        log.error("Failed to locate schema for topic %s", e)
        raise

    key_reader = avro.io.DatumReader(avro.schema.parse(key_schema))
    value_reader = avro.io.DatumReader(avro.schema.parse(value_schema))

    local = pyarrow.fs.LocalFileSystem()
    with local.open_input_file(archive) as f:
        with pyarrow.RecordBatchFileReader(f) as reader:
            table = reader.read_all()

    num_messages = table.num_rows
    key_bytes = 0
    value_bytes = 0
    num_ops: typing.DefaultDict[str, int] = collections.defaultdict(int)

    for i in range(0, table.num_rows):
        key = table["key"][i].as_py()
        value = table["value"][i].as_py()
        timestamp = table["timestamp"][i].value

        if key:
            key_bytes += len(key)
        if value:
            value_bytes += len(value)

        # Strip the first 5 bytes, as they are added by Confluent Platform and are not part of the
        # actual serialized data
        decoded = {
            "key": decode(key_reader, key[5:]),
            "timestamp": timestamp,
            "value": decode(value_reader, value[5:]),
        }

        op = decoded["value"]["op"]
        num_ops[op] += 1

    print(
        f"{topic},{num_messages},{key_bytes},{value_bytes},{num_ops['c']},{num_ops['u']},{num_ops['d']}"
    )


def summarize_archives(args: argparse.Namespace) -> None:
    """Print interesting statistics about archived Kafka topics."""

    topic_archives = glob.glob(f"{args.topic_filter}.arrow")
    if not topic_archives:
        log.error("No archives matching filter %s", args.topic_filter)
        sys.exit(1)

    print("Topic,NumMessages,KeyBytes,ValueBytes,NumCreates,NumUpdates,NumDeletes")
    with multiprocessing.Pool(args.num_procs) as pool:
        pool.map(summarize_archive, topic_archives)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-n",
        "--num-procs",
        help="Number of parallel processes to spawn",
        type=int,
        default=multiprocessing.cpu_count(),
    )

    parser.add_argument(
        "-t",
        "--topic-filter",
        help="Only restore messagges from topics that match filter string",
        type=str,
        default="debezium.tpcch.*",
    )

    args = parser.parse_args()
    summarize_archives(args)


if __name__ == "__main__":
    main()
