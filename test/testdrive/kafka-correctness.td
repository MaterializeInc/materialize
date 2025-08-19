# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

# Create a topic that is large enough to fill librdkafka's buffer, which will force some yielding to
# happen. Each message is at least 128 bytes long so writing 1M of them produces at least 128MB of
# data. Each of these million records sets the key="1" to the current iteration index.
$ set count=1000000
$ kafka-create-topic topic=correctness-data
$ kafka-ingest format=avro topic=correctness-data key-format=avro key-schema=${keyschema} schema=${schema} repeat=${count} start-iteration=1
{"key": "1"} {"f1": "some value that is 128 bytes loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong", "f2": ${kafka-ingest.iteration} }

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# Now create an UPSERT source and immediately after that a sink. The goal here is for the sink to
# get an AS_OF timestamp immediately, before the source has had the chance to produce data and
# compact. This means that the sink will observe all the state changes.
> CREATE CLUSTER correctness_data_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE correctness_data
  IN CLUSTER correctness_data_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-correctness-data-${testdrive.seed}')

> CREATE TABLE correctness_data_tbl FROM SOURCE correctness_data (REFERENCE "testdrive-correctness-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
> COMMIT

> CREATE CLUSTER correctness_sink_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK correctness_sink
  IN CLUSTER correctness_sink_cluster
  FROM correctness_data_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-correctness-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# If we upheld correctness property 2 then the sink should produce exactly *one* record. The record
# should be the accumulation of the snapshot
$ kafka-verify-data format=avro sink=materialize.public.correctness_sink sort-messages=true
{"before": null, "after": {"row": {"key": "1", "f1": "some value that is 128 bytes loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong", "f2": ${count} }}}
