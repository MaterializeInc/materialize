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

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=1
{"key": "bird1"} {"f1":"goose", "f2": 1}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=2
{"key": "birdmore"} {"f1":"geese", "f2": 2}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=3
{"key": "mammal1"} {"f1": "moose", "f2": 1}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=4
{"key": "bird1"} null

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=5
{"key": "birdmore"} {"f1":"geese", "f2": 56}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=6
{"key": "mammalmore"} {"f1": "moose", "f2": 42}

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=7
{"key": "mammal1"} null

> CREATE MATERIALIZED SOURCE avroavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avroavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

#> SELECT * from avroavro
#key           f1       f2
#-------------------------
#birdmore      geese    56
#mammalmore    moose    42

$ kafka-create-topic topic=textavro

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true timestamp=1
bird1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}
bird1: null
birdmore: {"f1":"geese", "f2": 56}
mammalmore: {"f1": "moose", "f2": 42}
mammal1: null

> CREATE MATERIALIZED SOURCE textavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT TEXT

#> select * from textavro
#key           f1       f2
#-------------------------
#birdmore      geese    56
#mammalmore    moose    42

> CREATE MATERIALIZED SOURCE bytesavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT FORMAT BYTES

#> select * from bytesavro
#key           f1       f2
#-------------------------
#birdmore      geese    56
#mammalmore    moose    42

$ kafka-create-topic topic=textbytes

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: publish=true
bìrd1:goose
birdmore:geese
mammal1:moose
bìrd1:
birdmore:géese
mammalmore:moose
mammal1:

> CREATE MATERIALIZED SOURCE textbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  FORMAT BYTES ENVELOPE UPSERT FORMAT TEXT

#> select * from textbytes
#key           data             mz_offset
#----------------------------------------
#birdmore      "g\\xc3\\xa9ese" 4
#mammalmore    "moose"          5

> CREATE MATERIALIZED SOURCE texttext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT

#> select * from texttext
#key           data  mz_offset
#-----------------------------
#bìrd1         goose 0
#birdmore      geese 1
#mammal1       moose 2
#birdmore      géese 4
#mammalmore    moose 5

> CREATE MATERIALIZED SOURCE bytesbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  FORMAT BYTES ENVELOPE UPSERT FORMAT BYTES

#> select * from bytesbytes
#key              data             mz_offset
#----------------------------------------
#"birdmore"       "g\\xc3\\xa9ese" 4
#"mammalmore"     "moose"          5

# A null key should result in an error decoding that row but not a panic
$ kafka-ingest format=bytes topic=nullkey key-format=bytes key-terminator=: publish=true
bird1:goose
:geese
mammal1:moose
bird1:
birdmore:geese
mammalmore:moose
mammal1:null

> CREATE MATERIALIZED SOURCE nullkey
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nullkey-${testdrive.seed}'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT

#> select * from nullkey
#key           data  mz_offset
#-----------------------------
#birdmore      geese 4
#mammalmore    moose 5
