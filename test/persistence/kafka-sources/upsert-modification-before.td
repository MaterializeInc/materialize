# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ingest records before restart and then upsert them to a different value post-restart
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=upsert-modification

$ kafka-ingest format=avro topic=upsert-modification key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": ${kafka-ingest.iteration}} {"f2": "${kafka-ingest.iteration}"}

> CREATE MATERIALIZED SOURCE upsert_modification
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upsert-modification-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM upsert_modification;
10000

$ kafka-create-topic topic=textbytes

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
fish:fish
bìrd1:goose
bírdmore:geese
mammal1:moose
bìrd1:

> CREATE MATERIALIZED SOURCE texttext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT TEXT VALUE FORMAT TEXT
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE textbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT TEXT VALUE FORMAT BYTES
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytesbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT BYTES VALUE FORMAT BYTES
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytestext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT BYTES VALUE FORMAT TEXT
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE UPSERT

> select * from texttext
key0          text  kafka_partition  mz_offset
----------------------------------------------
fish          fish  0                1
bírdmore      geese 0                3
mammal1       moose 0                4

> select * from textbytes
key0          data  kafka_partition  mz_offset
----------------------------------------------
fish          fish  0                1
bírdmore      geese 0                3
mammal1       moose 0                4

> select * from bytestext
key0            text  kafka_partition  mz_offset
------------------------------------------------
fish            fish  0                1
b\xc3\xadrdmore geese 0                3
mammal1         moose 0                4

> select * from bytesbytes
key0            data  kafka_partition  mz_offset
------------------------------------------------
fish            fish  0                1
b\xc3\xadrdmore geese 0                3
mammal1         moose 0                4
