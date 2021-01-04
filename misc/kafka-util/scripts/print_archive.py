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

# Script to read all messages in an arrow archive of a kafka topic and print the python objects to
# stdout

import argparse
import io
import json
import logging
import glob
import os
import pathlib
import pprint
import sys
import typing

import avro.io
import avro.schema
import pyarrow  # type: ignore
import pyarrow.fs  # type: ignore

# Setup basic formatting for logging output
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("print.records")
log.setLevel(logging.INFO)


def decode(reader: avro.io.DatumReader, msg: bytes):
    decoder = avro.io.BinaryDecoder(io.BytesIO(msg))
    return reader.read(decoder)


def decode_archive(args: argparse.Namespace) -> None:

    printer = pprint.pprint if args.pretty_print else print

    topic = args.topic

    with open("schemas.json") as fd:
        schemas = json.load(fd)

    try:
        key_schema = schemas[f"{topic}-key"]["schema"]
        value_schema = schemas[f"{topic}-value"]["schema"]
        if args.print_schemas:
            printer(json.loads(key_schema))
            printer(json.loads(value_schema))
    except KeyError:
        log.error("Failed to locate schema for topic {topic}")
        raise

    key_reader = avro.io.DatumReader(avro.schema.parse(key_schema))
    value_reader = avro.io.DatumReader(avro.schema.parse(value_schema))

    arrow_file = pathlib.Path(f"{topic}.arrow")
    if not arrow_file.exists():
        log.error(f"No arrow file for topic {topic}")
        sys.exit(1)

    local = pyarrow.fs.LocalFileSystem()
    with local.open_input_file(f"{topic}.arrow") as f:
        with pyarrow.RecordBatchFileReader(f) as reader:
            table = reader.read_all()

    for i in range(args.offset, min(table.num_rows, args.offset + args.count)):
        key = table["key"][i].as_py()
        value = table["value"][i].as_py()
        timestamp = table["timestamp"][i].value

        # Strip the first 5 bytes, as they are added by Kafka
        decoded = {
            "key": decode(key_reader, key[5:]),
            "timestamp": timestamp,
            "value": decode(value_reader, value[5:]),
        }

        printer(decoded)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c",
        "--count",
        help="Number of messages to print",
        type=int,
        default=float("inf"),
    )

    parser.add_argument(
        "-o",
        "--offset",
        help="Starting offset to print messages from",
        type=int,
        default=0,
    )

    parser.add_argument(
        "-p",
        "--pretty-print",
        help="Whether or not to print messages over multiple-lines using a pretty printer",
        action="store_true",
    )

    parser.add_argument(
        "-s",
        "--print-schemas",
        help="Whether or not to print schema definitions",
        action="store_true",
    )

    parser.add_argument(
        "topic", help="Name of the topic", type=str, nargs="?",
    )

    args = parser.parse_args()
    decode_archive(args)


if __name__ == "__main__":
    main()
