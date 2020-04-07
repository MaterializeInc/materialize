# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO: after taking up the arrange_from_upsert operator, the results of selecting
# from the streams should change.

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
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"} null
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"} null

#> CREATE MATERIALIZED SOURCE avroavro
#  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avroavro-${testdrive.seed}'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
#  ENVELOPE UPSERT

#> SELECT * from avroavro
#key           f1       f2
#-------------------------
#bird1         goose    1
#birdmore      geese    2
#mammal1       moose    1
#birdmore      geese    56
#mammalmore    moose    42

$ kafka-create-topic topic=textavro

$ kafka-ingest format=avro topic=textavro key-format=bytes schema=${schema} publish=true timestamp=1
bird1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}
bird1: null
birdmore: {"f1":"geese", "f2": 56}
mammalmore: {"f1": "moose", "f2": 42}
mammal1: null

#> CREATE MATERIALIZED SOURCE textavro
#  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
#  ENVELOPE UPSERT FORMAT TEXT

#> select * from textavro
#key           f1       f2
#-------------------------
#bird1         goose    1
#birdmore      geese    2
#mammal1       moose    1
#birdmore      geese    56
#mammalmore    moose    42
