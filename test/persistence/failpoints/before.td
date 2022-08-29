# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# A general test for use with failpoints. We ingest some data, then we activate
# a parameterized failpoint and then we ingest some more.
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=failpoint partitions=5

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "b${kafka-ingest.iteration}"} {"f2": "b${kafka-ingest.iteration}"}

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE failpoint
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-failpoint-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE KEY AS f1
  ENVELOPE UPSERT

> CREATE SINK failpoint_sink FROM failpoint
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-failpoint-sink-${testdrive.seed}'
  KEY (f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "c${kafka-ingest.iteration}"} {"f2": "c${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "d${kafka-ingest.iteration}"} {"f2": "d${kafka-ingest.iteration}"}

# Make sure that we read (and persisted) at least one message before activating the failpoint.
> SELECT COUNT(*) > 0 FROM failpoint;
true

> SET failpoints = '${arg.failpoint}=${arg.action}';

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "e${kafka-ingest.iteration}"} {"f2": "e${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "f${kafka-ingest.iteration}"} {"f2": "f${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "g${kafka-ingest.iteration}"} {"f2": "g${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "h${kafka-ingest.iteration}"} {"f2": "h${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "i${kafka-ingest.iteration}"} {"f2": "i${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "j${kafka-ingest.iteration}"} {"f2": "j${kafka-ingest.iteration}"}
