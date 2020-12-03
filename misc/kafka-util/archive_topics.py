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
import logging
import os
import sys

import kafka
import pyarrow
import pyarrow.fs
import requests

# Setup basic formatting for logging output
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger('restore.topics')
log.setLevel(logging.INFO)

# We only care to record the key, value, timestamp fields because the rest are empty or can be computed
SCHEMA = pyarrow.schema([('key', pyarrow.binary()),
                         ('value', pyarrow.binary()),
                         ('timestamp', pyarrow.timestamp('ms')),
                        ])

def archive_schema(args, topic):
    """Record the raw value of the key/value schema fields for the named topic."""

    for field in ['key', 'value']:
        response = requests.get(f"http://{args.schemahost}:8081/subjects/{topic}-{field}/versions/latest")
        if response.status_code != 200:
            log.warning(f"WARNING: Failed to query {field} schema for {topic}")
            continue

        outfile = os.path.join(topic, f"{field}.schema")
        with open(outfile, 'w') as f:
            f.write(response.json()['schema'])

def archive_topic(args, topic):

    consumer = kafka.KafkaConsumer(topic,
                                   group_id='archive.topics',
                                   auto_offset_reset='earliest',
                                   consumer_timeout_ms=1000,
                                   bootstrap_servers=[f'{args.kafkahost}:{args.port}'],
                                   enable_auto_commit=True)

    keys = []
    values = []
    timestamps = []
    for message in consumer:

        assert message.topic == topic, f"Expected topic name {topic}, got {message.topic}"
        assert message.partition == 0, f"Expected partition 0, got {message.partion}"
        assert message.timestamp_type == 0, "Expected timestamp to be 0 (CreateTime) but got {message.timestamp_type}"
        assert message.headers == [], f"Expected empty list of headers, got {message.headers}"
        assert message.serialized_header_size == -1, f"Expected negative serialized header size, got {message.serialized_header_size}"

        keys.append(message.key)
        values.append(message.value)
        timestamps.append(message.timestamp)

    data = [pyarrow.array(keys, type=pyarrow.binary()),
            pyarrow.array(values, type=pyarrow.binary()),
            pyarrow.array(timestamps, type=pyarrow.timestamp('ms')),
           ]

    table = pyarrow.Table.from_arrays(data, schema=SCHEMA)

    local = pyarrow.fs.LocalFileSystem()

    outfile = os.path.join(topic, "messages.arrow")
    with local.open_output_stream(outfile) as f:
        with pyarrow.RecordBatchFileWriter(f, table.schema) as writer:
            writer.write_table(table)

    log.info(f'Topic {topic} archived to local file')

def archive_topics(args):

    consumer = kafka.KafkaConsumer(bootstrap_servers=[f'{args.kafkahost}:{args.port}'],
                                   group_id='archive.topics')
    topics = sorted([t for t in consumer.topics() if t.startswith(args.topic_prefix)])

    for topic in topics:
        if os.path.exists(topic):
            log.error(f'ERROR: {topic} directory already exists; will not overwrite')
            sys.exit(1)

    for topic in topics:
        log.info(f'Archiving {topic}')
        os.mkdir(topic)
        archive_schema(args, topic)
        archive_topic(args, topic)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-k', '--kafkahost', help='Hostname of the Kafka Broker', type=str,
                        default='kafka')
    parser.add_argument('-s', '--schemahost', help='Hostname of the Schema Registry', type=str,
                        default='schema-registry')
    parser.add_argument('-p', '--port', help='Port to use for connecting to the Kafka Broker', type=int,
                        default=9092)
    parser.add_argument('-t', '--topic-prefix', help='Filter topics by prefix string', type=str,
                        default='debezium.tpcch')

    args = parser.parse_args()
    archive_topics(args)

if __name__ == '__main__':
    main()
