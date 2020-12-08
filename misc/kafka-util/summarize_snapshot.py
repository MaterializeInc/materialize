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
import json
import logging
import glob
import os
import sys

import kafka
import pyarrow
import pyarrow.fs
import requests

# Setup basic formatting for logging output
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("summarize.snapshots")
log.setLevel(logging.INFO)


def summarize_snapshot(args, snapshot):

    local = pyarrow.fs.LocalFileSystem()
    with local.open_input_file(snapshot) as f:
        with pyarrow.RecordBatchFileReader(f) as reader:
            table = reader.read_all()

    topic = os.path.splitext(snapshot)[0]
    num_messages = table.num_rows
    key_bytes = sum([len(k.as_py()) for k in table["key"] if k.as_py() is not None])
    value_bytes = sum([len(v.as_py()) for v in table["value"] if v.as_py() is not None])
    print(f"{topic},{num_messages},{key_bytes},{value_bytes}")


def summarize_snapshots(args):

    topic_snapshots = glob.glob(f"{args.topic_filter}.arrow")
    if not topic_snapshots:
        log.error(f"No snapshots matching filter {args.topic_filter}")
        sys.exit(1)

    print("Topic,NumMessages,KeyBytes,ValueBytes")
    for snapshot in topic_snapshots:
        summarize_snapshot(args, snapshot)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t",
        "--topic-filter",
        help="Only restore messagges from topics that match filter string",
        type=str,
        default="debezium.tpcch.*",
    )

    args = parser.parse_args()
    summarize_snapshots(args)


if __name__ == "__main__":
    main()
