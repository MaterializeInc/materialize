# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1minute

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

$ set consistency={
     "name": "materialize.byo.consistency",
     "type": "record",
     "fields": [
         {
           "name": "source",
           "type": "string"
         },
         {
           "name": "partition_count",
           "type": "int"
         },
         {
           "name": "partition_id",
           "type": ["int","string"]
         },
         {
            "name": "timestamp",
            "type": "long"
         },
         {
           "name": "offset",
           "type": "long"
         }
     ]
  }

$ kafka-create-topic topic=avroavro

$ kafka-create-topic topic=data-consistency

$ kafka-ingest format=avro topic=data-consistency schema=${consistency}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 1, "offset": 1}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 2, "offset": 2}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 3, "offset": 3}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 4, "offset": 4}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 5, "offset": 5}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 6, "offset": 6}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 7, "offset": 7}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 8, "offset": 8}
{"source": "testdrive-avroavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 9, "offset": 9}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"key": "fish"} {"f1": "fish", "f2": 1000}
{"key": "bird1"} {"f1":"goose", "f2": 1}
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"}
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"}
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

$ kafka-ingest format=avro topic=data-consistency schema=${consistency}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 1, "offset": 2}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 2, "offset": 4}

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
fish: {"f1": "fish", "f2": 1000}
bìrd1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}
bìrd1:
birdmore: {"f1":"geese", "f2": 56}
mämmalmore: {"f1": "moose", "f2": 42}
mammal1:
mammalmore: {"f1":"moose", "f2": 2}

> CREATE MATERIALIZED SOURCE bytesavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA  '${schema}'
  ENVELOPE UPSERT FORMAT BYTES

$ file-append path=data-schema.json
\${schema}

> CREATE MATERIALIZED SOURCE textavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textavro-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE UPSERT FORMAT TEXT

> select * from bytesavro
key           f1       f2
---------------------------
fish          fish     1000
b\xc3\xacrd1  goose    1
birdmore      geese    2
mammal1       moose    1

$ kafka-ingest format=avro topic=data-consistency schema=${consistency}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 3, "offset": 5}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 5, "offset": 6}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 8, "offset": 7}
{"source": "testdrive-textavro-${testdrive.seed}", "partition_count": 1, "partition_id": 0, "timestamp": 13, "offset": 8}

> select * from textavro
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mämmalmore    moose    42

$ kafka-create-topic topic=data-consistency2
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
    WITH (consistency = 'testdrive-data-consistency2-${testdrive.seed}')
    FORMAT TEXT ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE textbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
  WITH (consistency = 'testdrive-data-consistency2-${testdrive.seed}')
  FORMAT BYTES ENVELOPE UPSERT FORMAT TEXT

> CREATE MATERIALIZED SOURCE bytesbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency2-${testdrive.seed}')
  FORMAT BYTES ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytestext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-textbytes-${testdrive.seed}'
      WITH (consistency = 'testdrive-data-consistency2-${testdrive.seed}')
  FORMAT TEXT ENVELOPE UPSERT FORMAT BYTES

$ kafka-ingest format=bytes topic=data-consistency2
testdrive-textbytes-${testdrive.seed},1,0,1,1
testdrive-textbytes-${testdrive.seed},1,0,3,3
testdrive-textbytes-${testdrive.seed},1,0,6,4
testdrive-textbytes-${testdrive.seed},1,0,10,5

> select * from texttext
key           data  mz_offset
-----------------------------
fish          fish  1
bírdmore      geese 3
mammal1       moose 4

$ kafka-ingest format=bytes topic=data-consistency2
testdrive-textbytes-${testdrive.seed},1,0,15,8
testdrive-textbytes-${testdrive.seed},1,0,21,9

> select * from textbytes
key           data             mz_offset
----------------------------------------
fish          fish             1
bírdmore      g\xc3\xa9ese     6
mammal1       mouse            9
mammalmore    moose            7

$ kafka-ingest format=bytes topic=data-consistency2
testdrive-textbytes-${testdrive.seed},1,0,28,10

> select * from bytesbytes
key              data             mz_offset
----------------------------------------
fish             fish             1
b\xc3\xadrdmore  g\xc3\xa9ese     6
mammal1          mouse            9
mammalmore       herd             10

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: publish=true
bìrd1:
fish:

$ kafka-ingest format=bytes topic=data-consistency2
testdrive-textbytes-${testdrive.seed},1,0,36,11
testdrive-textbytes-${testdrive.seed},1,0,45,12

> select * from bytestext
key              data             mz_offset
----------------------------------------
b\xc3\xadrdmore  géese            6
mammal1          mouse            9
mammalmore       herd             10

$ kafka-create-topic topic=nullkey

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
      WITH (consistency = 'testdrive-data-consistency2-${testdrive.seed}')
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT

$ kafka-ingest format=bytes topic=data-consistency2
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
birdmore      geese 5
mammalmore    moose 6

$ kafka-create-topic topic=realtimeavroavro

# test multi-field avro key
$ set keyschema2={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f3", "type": ["null", "string"]},
        {"name": "f1", "type": ["null", "string"]}
    ]
  }

$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": "fire", "f1": "yang"} {"f1": "dog", "f2": 42}
{"f3": null, "f1": "yin"} {"f1": "sheep", "f2": 53}
{"f3": "water", "f1": null} {"f1":"plesiosaur", "f2": 224}
{"f3": "earth", "f1": "dao"} {"f1": "turtle", "f2": 34}
{"f3": null, "f1": "yin"} {"f1": "sheep", "f2": 54}
{"f3": "earth", "f1": "dao"} {"f1": "snake", "f2": 68}
{"f3": "water", "f1": null} {"f1": "crocodile", "f2": 7}
{"f3": "earth", "f1":"dao"}

> CREATE SOURCE realtimeavroavro (f3, f4, f1, f2)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-realtimeavroavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED VIEW realtimeavroavro_view as SELECT * from realtimeavroavro;

> select f3, f4, f1, f2 from realtimeavroavro_view
f3        f4      f1             f2
-----------------------------------
fire      yang    dog            42
<null>    yin     sheep          54
water     <null>  crocodile      7

# Ensure that Upsert sources work with `start_offset`
> CREATE MATERIALIZED SOURCE realtimeavroavro_ff
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-realtimeavroavro-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT * FROM realtimeavroavro_ff
f3        f4        f1           f2
-----------------------------------
<null>    yin       sheep        54
water     <null>    crocodile    7

# ensure that having deletion on a key that never existed does not break anything
$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": "fire", "f1": "yin"}
{"f3": "air", "f1":"qi"} {"f1": "pigeon", "f2": 10}
{"f3": "air", "f1":"qi"} {"f1": "owl", "f2": 15}
{"f3": "earth", "f1": "dao"} {"f1": "rhinoceros", "f2": 211}
{"f3": "air", "f1":"qi"} {"f1": "chicken", "f2": 47}
{"f3": null, "f1":"yin"}
{"f3": null, "f1":"yin"} {"f1":"dog", "f2": 243}
{"f3": "water", "f1": null}

> select * from realtimeavroavro_view
key        f4          f1             f2
-----------------------------------------
fire       yang        dog            42
air        qi          chicken        47
<null>     yin         dog            243
earth      dao         rhinoceros     211
