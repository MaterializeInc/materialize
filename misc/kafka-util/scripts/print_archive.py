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
import pathlib
import pprint
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
log = logging.getLogger("print.records")


def decode(reader: avro.io.DatumReader, msg: bytes) -> typing.Any:
    """Return the Python object represented by the avro-encoded message."""
    decoder = avro.io.BinaryDecoder(io.BytesIO(msg))
    return reader.read(decoder)


def print_archive(args: argparse.Namespace) -> None:
    """Print the decoded representation of messages in an archive file."""

    # Why not 'printer = pprint.pprint if args.pretty_print else print'?
    # Because mypy exits with 'error: Cannot call function of unknown type'
    def printer(obj: typing.Any) -> None:
        if args.pretty_print:
            pprint.pprint(obj)
        else:
            print(obj)

    topic = args.topic

    with open("schemas.json") as fd:
        schemas = json.load(fd)

    try:
        key_schema = schemas[f"{topic}-key"]["schema"]
        value_schema = schemas[f"{topic}-value"]["schema"]
        if args.print_schemas:
            printer(json.loads(key_schema))
            printer(json.loads(value_schema))
    except KeyError as e:
        log.error("Failed to locate schema for topic: %s", e)
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

        # Strip the first 5 bytes, as they are added by Confluent Platform and are not part of the
        # actual serialized data
        decoded = {
            "key": decode(key_reader, key[5:]) if key else None,
            "timestamp": timestamp,
            "value": decode(value_reader, value[5:]) if value else None,
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

    parser.add_argument("topic", help="Name of the topic", type=str)

    args = parser.parse_args()
    print_archive(args)


if __name__ == "__main__":
    main()
