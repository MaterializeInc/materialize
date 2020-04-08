# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO: uncomment tests when timestamping problem is fixed.
# TODO: add tests where the updated value is less than old value

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema=[
    "null",
    {
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }
  ]

$ kafka-create-topic topic=avroavro

$ kafka-ingest format=bytes topic=data-consistency
testdrive-avroavro-${testdrive.seed},1,0,1,1
testdrive-avroavro-${testdrive.seed},1,0,2,2
testdrive-avroavro-${testdrive.seed},1,0,3,3
testdrive-avroavro-${testdrive.seed},1,0,4,4
testdrive-avroavro-${testdrive.seed},1,0,5,5
testdrive-avroavro-${testdrive.seed},1,0,6,6
testdrive-avroavro-${testdrive.seed},1,0,7,7
testdrive-avroavro-${testdrive.seed},1,0,8,8
testdrive-avroavro-${testdrive.seed},1,0,9,9

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "fish"} {"f1": "fish", "f2": 1000}
{"key": "bird1"} {"f1":"goose", "f2": 1}
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"} null
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"} null
{"key": "mammalmore"} {"f1":"moose", "f2": 2}

> CREATE MATERIALIZED SOURCE avroavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-avroavro-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT * from avroavro
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mammalmore    moose    2

$ kafka-create-topic topic=textavro

$ kafka-ingest format=bytes topic=data-consistency
testdrive-textavro-${testdrive.seed},1,0,1,1
testdrive-textavro-${testdrive.seed},1,0,1,2
testdrive-textavro-${testdrive.seed},1,0,2,4

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
fish: {"f1": "fish", "f2": 1000}
bìrd1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}
bìrd1: null
birdmore: {"f1":"geese", "f2": 56}
mämmalmore: {"f1": "moose", "f2": 42}
mammal1: null
mammalmore: {"f1":"moose", "f2": 2}

> CREATE MATERIALIZED SOURCE bytesavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT BYTES

> CREATE MATERIALIZED SOURCE textavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textavro-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT TEXT

> select * from bytesavro
key           f1       f2
---------------------------
fish          fish     1000
b\xc3\xacrd1  goose    1
birdmore      geese    2
mammal1       moose    1

$ kafka-ingest format=bytes topic=data-consistency
testdrive-textavro-${testdrive.seed},1,0,3,5
testdrive-textavro-${testdrive.seed},1,0,5,6
testdrive-textavro-${testdrive.seed},1,0,8,7
testdrive-textavro-${testdrive.seed},1,0,13,8

> select * from textavro
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mämmalmore    moose    42

$ kafka-create-topic topic=textbytes

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: publish=true
fish:fish
bìrd1:goose
bírdmore:geese
mammal1:moose
bìrd1:
bírdmore:géese
mammalmore:moose
mammal1:
mammal1:mouse
mammalmore:herd

> CREATE MATERIALIZED SOURCE texttext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT

> CREATE MATERIALIZED SOURCE textbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT BYTES ENVELOPE UPSERT FORMAT TEXT

> CREATE MATERIALIZED SOURCE bytesbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT BYTES ENVELOPE UPSERT FORMAT BYTES

$ kafka-ingest format=bytes topic=data-consistency
testdrive-textbytes-${testdrive.seed},1,0,1,1
testdrive-textbytes-${testdrive.seed},1,0,3,3
testdrive-textbytes-${testdrive.seed},1,0,6,4
testdrive-textbytes-${testdrive.seed},1,0,10,5

> select * from texttext
key           data  mz_offset
-----------------------------
fish          fish  0
bírdmore      geese 2
mammal1       moose 3

$ kafka-ingest format=bytes topic=data-consistency
testdrive-textbytes-${testdrive.seed},1,0,15,8
testdrive-textbytes-${testdrive.seed},1,0,21,9

> select * from textbytes
key           data             mz_offset
----------------------------------------
fish          fish             0
bírdmore      g\xc3\xa9ese     5
mammal1       mouse            8
mammalmore    moose            6

$ kafka-ingest format=bytes topic=data-consistency
testdrive-textbytes-${testdrive.seed},1,0,28,10

> select * from bytesbytes
key              data             mz_offset
----------------------------------------
fish             fish             0
b\xc3\xadrdmore  g\xc3\xa9ese     5
mammal1          mouse            8
mammalmore       herd             9

# A null key should result in an error decoding that row but not a panic
$ kafka-ingest format=bytes topic=nullkey key-format=bytes key-terminator=: publish=true
bird1:goose
:geese
mammal1:moose
bird1:
birdmore:geese
mammalmore:moose
mammal1:

> CREATE MATERIALIZED SOURCE nullkey
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-nullkey-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT

$ kafka-ingest format=bytes topic=data-consistency
testdrive-nullkey-${testdrive.seed},1,0,30,1
testdrive-nullkey-${testdrive.seed},1,0,32,2
testdrive-nullkey-${testdrive.seed},1,0,35,3
testdrive-nullkey-${testdrive.seed},1,0,38,4
testdrive-nullkey-${testdrive.seed},1,0,42,5
testdrive-nullkey-${testdrive.seed},1,0,46,6
testdrive-nullkey-${testdrive.seed},1,0,51,7

> select * from nullkey
key           data  mz_offset
-----------------------------
birdmore      geese 4
mammalmore    moose 5
