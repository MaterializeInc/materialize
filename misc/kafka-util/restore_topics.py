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

# Script to list and archive all topics from a Kafka cluster
# Writes topic contents as Arrow encoded Tables into the working directory of the script

import argparse
import datetime
import json
import logging
import glob
import queue
import os
import sys
import time

import pykafka
import pyarrow
import pyarrow.fs
import requests

# Setup basic formatting for logging output
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("restore.topics")
log.setLevel(logging.INFO)


def load_sorted_schemas():
    """Load the list of schemas, sorted by schema registry's internal ID."""

    with open("schemas.json") as fd:
        schemas = json.load(fd)

    yield from sorted(
        (definition["id"], subject, definition["schema"])
        for subject, definition in schemas.items()
    )


def restore_schemas(args):
    """Create key and value schemas for this topic."""

    for subject_id, subject, schema in load_sorted_schemas():

        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        response = requests.post(
            f"http://{args.schemahost}:8081/subjects/{subject}/versions",
            json={"schema": schema},
            headers=headers,
        )

        response.raise_for_status()
        if not response.json()["id"] == subject_id:
            log.fatal(f"Failed to reproduce subject {subject} with id {subject_id}")
            sys.exit(1)


def restore_topic(args, archive):

    topic = os.path.splitext(archive)[0]
    log.info(f"Restoring messages to topic {topic}")

    client = pykafka.KafkaClient(f"{args.kafkahost}:{args.port}", broker_version="1.0.0")

    local = pyarrow.fs.LocalFileSystem()

    with local.open_input_file(f"{topic}.arrow") as f:
        with pyarrow.RecordBatchFileReader(f) as reader:
            table = reader.read_all()

    log.info(f"Loaded {table.num_rows} from snapshot file")

    kafka_topic = client.topics[topic]
    with kafka_topic.get_producer(max_retries=0, delivery_reports=True, use_rdkafka=True) as producer:

        total_msgs = table.num_rows

        for i in range(0, table.num_rows):
            key = table["key"][i].as_py()
            value = table["value"][i].as_py()
            timestamp = table["timestamp"][i].value
            (sec, ms) = divmod(timestamp, 1000)
            dt = datetime.datetime.fromtimestamp(sec).replace(microsecond=ms)

            producer.produce(value, partition_key=key, timestamp=dt)

    log.info(f"Restored {table.num_rows} rows to topic {topic}")


def restore_topics(args):

    topic_archives = glob.glob(f"{args.topic_filter}.arrow")
    if not topic_archives:
        log.error(f"No topics matching filter {args.topic_filter}")
        sys.exit(1)

    # Restore all schemas so that Peeker can create sources properly and so that Avro decoding
    # works correctly (schema ID, not subject name, is embedded in each message)
    restore_schemas(args)

    for archive in topic_archives:
        restore_topic(args, archive)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-k",
        "--kafkahost",
        help="Hostname of the Kafka Broker",
        type=str,
        default="kafka",
    )
    parser.add_argument(
        "-s",
        "--schemahost",
        help="Hostname of the Schema Registry",
        type=str,
        default="schema-registry",
    )
    parser.add_argument(
        "-p",
        "--port",
        help="Port to use for connecting to the Kafka Broker",
        type=int,
        default=9092,
    )
    parser.add_argument(
        "-t",
        "--topic-filter",
        help="Only restore messagges from topics that match filter string",
        type=str,
        default="debezium.tpcch.*",
    )

    args = parser.parse_args()
    restore_topics(args)


if __name__ == "__main__":
    main()
