# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This testdrive file is exactly the same as upsert-kafka.td, but using the new syntax.

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

$ kafka-create-topic topic=avroavro

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
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avroavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT * from avroavro
key           f1       f2    mz_offset
--------------------------------------
fish          fish     1000  1
birdmore      geese    56    6
mammalmore    moose    2     9

$ kafka-create-topic topic=textavro

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
fish: {"f1": "fish", "f2": 1000}
bìrd1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}

> CREATE MATERIALIZED SOURCE bytesavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT AVRO USING SCHEMA  '${schema}'
  ENVELOPE UPSERT

$ file-append path=data-schema.json
\${schema}

> CREATE MATERIALIZED SOURCE textavro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textavro-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE UPSERT

> select * from bytesavro
key0          f1       f2    mz_offset
--------------------------------------
fish          fish     1000  1
b\xc3\xacrd1  goose    1     2
birdmore      geese    2     3
mammal1       moose    1     4

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
bìrd1:
birdmore: {"f1":"geese", "f2": 56}
mämmalmore: {"f1": "moose", "f2": 42}
mammal1:

> select * from textavro
key0          f1       f2    mz_offset
--------------------------------------
fish          fish     1000  1
birdmore      geese    56    6
mämmalmore    moose    42    7

$ kafka-create-topic topic=textbytes

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
fish:fish
bìrd1:goose
bírdmore:geese
mammal1:moose
bìrd1:

> CREATE MATERIALIZED SOURCE texttext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  FORMAT TEXT ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE textbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytesbytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  FORMAT BYTES ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytestext
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

> select * from texttext
key0          text  mz_offset
-----------------------------
fish          fish  1
bírdmore      geese 3
mammal1       moose 4

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
bírdmore:géese
mammalmore:moose
mammal1:
mammal1:mouse

> select * from textbytes
key0          data             mz_offset
----------------------------------------
fish          fish             1
bírdmore      g\xc3\xa9ese     6
mammal1       mouse            9
mammalmore    moose            7

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
mammalmore:herd

> select * from bytesbytes
key0             data             mz_offset
----------------------------------------
fish             fish             1
b\xc3\xadrdmore  g\xc3\xa9ese     6
mammal1          mouse            9
mammalmore       herd             10

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
bìrd1:
fish:

> select * from bytestext
key0             text             mz_offset
----------------------------------------
b\xc3\xadrdmore  géese            6
mammal1          mouse            9
mammalmore       herd             10

$ kafka-create-topic topic=textproto

> CREATE MATERIALIZED SOURCE textproto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textproto-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE bytesproto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-textproto-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT PROTOBUF MESSAGE '.Struct' USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'
  ENVELOPE UPSERT

$ kafka-ingest format=protobuf message=struct topic=textproto key-format=bytes key-terminator=:
fish:{"int": 1, "bad_int": 1, "bin": "ONE", "st": "my-string"}
bìrd1:{"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}
birdmore: {"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}

> SELECT * FROM bytesproto
fish 1 1 ONE my-string 1
b\xc3\xacrd1 2 2 ONE something-valid 2
birdmore 2 2 ONE something-valid 3

> SELECT * FROM textproto
fish 1 1 ONE my-string 1
bìrd1 2 2 ONE something-valid 2
birdmore 2 2 ONE something-valid 3

$ kafka-ingest format=protobuf message=struct topic=textproto key-format=bytes key-terminator=:
mammal1: {"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}
bìrd1:
birdmore: {"int": 2, "bad_int": 2, "bin": "ONE", "st": "stuff"}
mämmalmore: {"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}
bìrd1:{"int": 2, "bad_int": 1, "bin": "ONE", "st": "something-valid"}
mammal1:
mammalmore: {"int": 2, "bad_int": 2, "bin": "ONE", "st": "something-valid"}

> SELECT * FROM bytesproto
fish 1 1 ONE my-string 1
birdmore 2 2 ONE stuff 6
m\xc3\xa4mmalmore 2 2 ONE something-valid 7
b\xc3\xacrd1 2 1 ONE something-valid 8
mammalmore 2 2 ONE something-valid 10

> SELECT * FROM textproto
fish 1 1 ONE my-string 1
birdmore 2 2 ONE stuff 6
mämmalmore 2 2 ONE something-valid 7
bìrd1 2 1 ONE something-valid 8
mammalmore 2 2 ONE something-valid 10

$ kafka-create-topic topic=nullkey

# A null key should result in an error decoding that row but not a panic
$ kafka-ingest format=bytes topic=nullkey key-format=bytes key-terminator=:
bird1:goose
:geese
mammal1:moose
bird1:
birdmore:geese
mammalmore:moose
mammal1:

> CREATE MATERIALIZED SOURCE nullkey
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-nullkey-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

> select * from nullkey
key0          text  mz_offset
-----------------------------
birdmore      geese 5
mammalmore    moose 6

$ kafka-create-topic topic=realtimeavroavro

# test multi-field avro key
$ set keyschema2={
    "type": "record",
    "name": "Key2",
    "fields": [
        {"name": "f3", "type": ["null", "string"]},
        {"name": "f1", "type": ["null", "string"]}
    ]
  }

$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": {"string": "fire"}, "f1": {"string": "yang"}} {"f1": "dog", "f2": 42}
{"f3": null, "f1": {"string": "yin"}} {"f1": "sheep", "f2": 53}
{"f3": {"string": "water"}, "f1": null} {"f1":"plesiosaur", "f2": 224}
{"f3": {"string": "earth"}, "f1": {"string": "dao"}} {"f1": "turtle", "f2": 34}
{"f3": null, "f1": {"string": "yin"}} {"f1": "sheep", "f2": 54}
{"f3": {"string": "earth"}, "f1": {"string": "dao"}} {"f1": "snake", "f2": 68}
{"f3": {"string": "water"}, "f1": null} {"f1": "crocodile", "f2": 7}
{"f3": {"string": "earth"}, "f1":{"string": "dao"}}

> CREATE SOURCE realtimeavroavro (f3, f4, f1, f2)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-realtimeavroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> CREATE MATERIALIZED VIEW realtimeavroavro_view as SELECT * from realtimeavroavro;

> select * from realtimeavroavro_view
f3        f4      f1             f2  mz_offset
----------------------------------------------
fire      yang    dog            42  1
<null>    yin     sheep          54  5
water     <null>  crocodile      7   7

# Ensure that Upsert sources work with `start_offset`
> CREATE MATERIALIZED SOURCE realtimeavroavro_ff
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-realtimeavroavro-${testdrive.seed}'
  WITH (start_offset=1)
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT * FROM realtimeavroavro_ff
f3        f1        f1           f2  mz_offset
----------------------------------------------
<null>    yin       sheep        54  5
water     <null>    crocodile    7   7

# ensure that having deletion on a key that never existed does not break anything
$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": {"string": "fire"}, "f1": {"string": "yin"}}
{"f3": {"string": "air"}, "f1":{"string": "qi"}} {"f1": "pigeon", "f2": 10}
{"f3": {"string": "air"}, "f1":{"string": "qi"}} {"f1": "owl", "f2": 15}
{"f3": {"string": "earth"}, "f1": {"string": "dao"}} {"f1": "rhinoceros", "f2": 211}
{"f3": {"string": "air"}, "f1":{"string": "qi"}} {"f1": "chicken", "f2": 47}
{"f3": null, "f1":{"string": "yin"}}
{"f3": null, "f1":{"string": "yin"}} {"f1":"dog", "f2": 243}
{"f3": {"string": "water"}, "f1": null}

> select * from realtimeavroavro_view
f3         f4          f1             f2   mz_offset
----------------------------------------------------
fire       yang        dog            42   1
air        qi          chicken        47   13
<null>     yin         dog            243  15
earth      dao         rhinoceros     211  12

$ kafka-create-topic topic=realtimefilteravro

$ set keyschema3={
    "type": "record",
    "name": "Key3",
    "fields": [
        {"name": "k1", "type": ["null", "string"]},
        {"name": "k2", "type": ["null", "long"]}
    ]
  }

$ set schema2={
    "type": "record",
    "name": "test2",
    "fields": [
        {"name": "f1", "type": ["null", "string"]},
        {"name": "f2", "type": ["null", "long"]}
    ]
  }

$ kafka-ingest format=avro topic=realtimefilteravro key-format=avro key-schema=${keyschema3} schema=${schema2} publish=true
{"k1": null, "k2": {"long": 2}} {"f1": {"string": "date"}, "f2": {"long": 5}}
{"k1": {"string": "épicerie"}, "k2": {"long": 10}} {"f1": {"string": "melon"}, "f2": {"long": 2}}
{"k1": {"string": "boucherie"}, "k2": {"long": 5}} {"f1": {"string": "apple"}, "f2": {"long": 7}}
{"k1": {"string": "boulangerie"}, "k2": null} {"f1":{"string": "date"}, "f2": {"long": 10}}
{"k1": {"string": "épicerie"}, "k2": {"long": 10}} {"f1": {"string": "pear"}, "f2": {"long": 2}}
{"k1": null, "k2": {"long": 2}} {"f1": {"string": "date"}, "f2": null}
{"k1": {"string": "boulangerie"}, "k2": null} {"f1":null, "f2": {"long": 10}}
{"k1": {"string": "boucherie"}, "k2": {"long": 5}} {"f1": {"string": "apple"}, "f2": {"long": 3}}

> CREATE SOURCE realtimefilteravro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-realtimefilteravro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

# filter on key only
> CREATE MATERIALIZED VIEW filterforkey AS
  SELECT f1 FROM realtimefilteravro WHERE k1='épicerie';

> SELECT * from filterforkey
f1
----
pear

# filter on value only
> CREATE MATERIALIZED VIEW filterforvalue AS
  SELECT f2 FROM realtimefilteravro WHERE f1='date';

> SELECT * from filterforvalue
f2
-------
<null>

# filter with a predicate containing key + value
> CREATE MATERIALIZED VIEW filterforkeyvalue AS
  SELECT f1, f2 FROM realtimefilteravro WHERE k2+f2=12;

> SELECT * from filterforkeyvalue
f1   f2
-------
pear 2

# filter on both a predicate containing a key and a predicate containing a value
> CREATE MATERIALIZED VIEW keyfiltervaluefilter AS
  SELECT k1, k2 FROM realtimefilteravro WHERE k2 > 5 AND f2 < 5

> SELECT * from keyfiltervaluefilter
k1       k2
-----------
épicerie 10

# add records that match the filter
# make sure that rows that differ on unneeded key columns are treated as separate
$ kafka-ingest format=avro topic=realtimefilteravro key-format=avro key-schema=${keyschema3} schema=${schema2} publish=true
{"k1": {"string": "librairie"}, "k2": {"long": 10}} {"f1":null, "f2": {"long": 2}}
{"k1": null, "k2": null} {"f1": {"string": "date"}, "f2": {"long": 5}}
{"k1": {"string": "épicerie"}, "k2": {"long": 6}} {"f1": {"string": "pear"}, "f2": null}
{"k1": {"string": "bureau"}, "k2": {"long": 6}} {"f1": {"string": "grape"}, "f2": {"long": 7}}

> SELECT * from filterforkey
f1
----
pear
pear

> SELECT * from filterforvalue
f2
-------
<null>
5

> SELECT * from filterforkeyvalue
f1     f2
---------
pear   2
<null> 2

> SELECT * from keyfiltervaluefilter
k1        k2
-----------
épicerie  10
librairie 10

# update records so that they don't match the filter
$ kafka-ingest format=avro topic=realtimefilteravro key-format=avro key-schema=${keyschema3} schema=${schema2} publish=true
{"k1": {"string": "librairie"}, "k2": {"long": 10}} {"f1":null, "f2": {"long": 6}}
{"k1": null, "k2": null} {"f1": {"string": "grape"}, "f2": {"long": 5}}

> SELECT * from filterforvalue
f2
-------
<null>

> SELECT * from filterforkeyvalue
f1     f2
---------
pear   2

> SELECT * from keyfiltervaluefilter
k1        k2
-----------
épicerie  10

# update records so that they do match the filter
$ kafka-ingest format=avro topic=realtimefilteravro key-format=avro key-schema=${keyschema3} schema=${schema2} publish=true
{"k1": {"string": "librairie"}, "k2": {"long": 10}} {"f1":{"string": "melon"}, "f2": {"long": 2}}
{"k1": null, "k2": null} {"f1": {"string": "date"}, "f2": {"long": 12}}

> SELECT * from filterforvalue
f2
-------
<null>
12

> SELECT * from filterforkeyvalue
f1     f2
---------
pear   2
melon  2

> SELECT * from keyfiltervaluefilter
k1        k2
-----------
épicerie  10
librairie 10

# delete records
$ kafka-ingest format=avro topic=realtimefilteravro key-format=avro key-schema=${keyschema3} schema=${schema2} publish=true
{"k1": {"string": "boucherie"}, "k2": {"long": 5}}
{"k1": {"string": "épicerie"}, "k2": {"long": 10}}
{"k1": {"string": "boulangerie"}, "k2": null}
{"k1": null, "k2": {"long": 2}}

> SELECT * from filterforkey
f1
----
pear

> SELECT * from filterforvalue
f2
-------
12

> SELECT * from filterforkeyvalue
f1     f2
---------
melon  2

> SELECT * from keyfiltervaluefilter
k1        k2
-----------
librairie 10
