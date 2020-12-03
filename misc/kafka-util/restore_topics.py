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
import glob
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

def restore_schema(args, topic):
    """Create key and value schemas for this topic."""

    for schemafile in glob.glob(os.path.join(topic, '*.schema')):
        field = os.path.splitext(os.path.basename(schemafile))[0]
        with open(schemafile) as f:
            contents = f.read()

        headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
        response = requests.post(f"http://{args.schemahost}:8081/subjects/{topic}-{field}/versions",
                                 json={'schema': contents},
                                 headers=headers)

        response.raise_for_status()

def restore_topic(args, topic):

    producer = kafka.KafkaProducer(bootstrap_servers=[f'{args.kafkahost}:{args.port}'],
                                   retries=3)

    def on_error(excp):
        log.error(f'ERROR: Failed to send message {excp}')
        sys.exit(1)

    local = pyarrow.fs.LocalFileSystem()

    with local.open_input_file(os.path.join(topic, 'messages.arrow')) as f:
        with pyarrow.RecordBatchFileReader(f) as reader:
            table = reader.read_all()

    for i in range(0, table.num_rows):
        key = table['key'][i].as_py()
        value = table['value'][i].as_py()
        timestamp = table['timestamp'][i].value

        producer.send(f'{topic}', key=key, value=value, timestamp_ms=timestamp).add_errback(on_error)

    producer.flush()
    log.info(f'Restored {table.num_rows} rows to topic {topic}')


def restore_topics(args):

    restore_topics = glob.glob(f'{args.topic_filter}')
    if not restore_topics:
        log.error(f'No topics matching filter {args.topic_filter}')
        sys.exit(1)

    # Restore all schemas so that Peeker can create sources properly
    for topic in glob.glob('debezium.*'):
        log.info(f'Restoring schema for topic {topic}')
        restore_schema(args, topic)

    for topic in restore_topics:
        log.info(f'Restoring messages to topic {topic}')
        restore_topic(args, topic)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-k', '--kafkahost', help='Hostname of the Kafka Broker', type=str,
                        default='kafka')
    parser.add_argument('-s', '--schemahost', help='Hostname of the Schema Registry', type=str,
                        default='schema-registry')
    parser.add_argument('-p', '--port', help='Port to use for connecting to the Kafka Broker', type=int,
                        default=9092)
    parser.add_argument('-t', '--topic-filter', help='Only restore messagges from topics that match filter string', type=str,
                        default='debezium.tpcch.*')

    args = parser.parse_args()
    restore_topics(args)

if __name__ == '__main__':
    main()
