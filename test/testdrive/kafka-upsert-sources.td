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

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

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

> CREATE SOURCE avroavro
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-avroavro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT * from avroavro
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mammalmore    moose    2

$ kafka-create-topic topic=textavro

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
fish: {"f1": "fish", "f2": 1000}
bìrd1: {"f1":"goose", "f2": 1}
birdmore: {"f1":"geese", "f2": 2}
mammal1: {"f1": "moose", "f2": 1}

> CREATE SOURCE bytesavro
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textavro-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT AVRO USING SCHEMA  '${schema}'
  ENVELOPE UPSERT

> CREATE SOURCE textavro
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textavro-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE UPSERT

> select * from bytesavro
key           f1       f2
---------------------------
fish          fish     1000
b\xc3\xacrd1  goose    1
birdmore      geese    2
mammal1       moose    1

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema} publish=true
bìrd1:
birdmore: {"f1":"geese", "f2": 56}
mämmalmore: {"f1": "moose", "f2": 42}
mammal1:

> select * from textavro
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mämmalmore    moose    42

$ kafka-create-topic topic=textbytes partitions=1

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
fish:fish
bìrd1:goose
bírdmore:geese
mammal1:moose
bìrd1:

> CREATE SOURCE texttext
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

> CREATE SOURCE textbytes
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> CREATE SOURCE bytesbytes
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> CREATE SOURCE bytestext
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textbytes-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

> select * from texttext
key           text
-------------------
fish          fish
bírdmore      geese
mammal1       moose

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
bírdmore:géese
mammalmore:moose
mammal1:
mammal1:mouse

> select * from textbytes
key           data
---------------------------
fish          fish
bírdmore      g\xc3\xa9ese
mammal1       mouse
mammalmore    moose

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
mammalmore:herd

> select * from bytesbytes
key              data
------------------------------
fish             fish
b\xc3\xadrdmore  g\xc3\xa9ese
mammal1          mouse
mammalmore       herd

$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
bìrd1:
fish:

> select * from bytestext
key              text
-----------------------
b\xc3\xadrdmore  géese
mammal1          mouse
mammalmore       herd

$ file-append path=test.proto
syntax = "proto3";

message Test {
    string f = 1;
}

$ protobuf-compile-descriptors inputs=test.proto output=test.pb

$ kafka-create-topic topic=textproto partitions=1

> CREATE SOURCE textproto
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textproto-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT PROTOBUF MESSAGE '.Test' USING SCHEMA FILE '${testdrive.temp-dir}/test.pb'
  ENVELOPE UPSERT

> CREATE SOURCE bytesproto
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-textproto-${testdrive.seed}'
  KEY FORMAT BYTES
  VALUE FORMAT PROTOBUF MESSAGE '.Test' USING SCHEMA FILE '${testdrive.temp-dir}/test.pb'
  ENVELOPE UPSERT

$ kafka-ingest topic=textproto
  format=protobuf descriptor-file=test.pb message=Test
  key-format=bytes key-terminator=:
fish:{"f": "one"}
bìrd1:{"f": "two"}
birdmore: {}

> SELECT * FROM bytesproto
fish         one
b\xc3\xacrd1 two
birdmore     ""

> SELECT * FROM textproto
fish      one
bìrd1     two
birdmore  ""

$ kafka-ingest topic=textproto
  format=protobuf descriptor-file=test.pb message=Test
  key-format=bytes key-terminator=:
mammal1: {"f": "three"}
bìrd1:
birdmore: {"f": "four"}
mämmalmore: {"f": "five"}
bìrd1: {"f": "six"}
mammal1:
mammalmore: {"f": "seven"}

> SELECT * FROM bytesproto
fish              one
birdmore          four
m\xc3\xa4mmalmore five
b\xc3\xacrd1      six
mammalmore        seven

> SELECT * FROM textproto
fish        one
birdmore    four
mämmalmore  five
bìrd1       six
mammalmore  seven

$ kafka-create-topic topic=nullkey partitions=1

# A null key should result in an error decoding that row but not a panic
$ kafka-ingest format=bytes topic=nullkey key-format=bytes key-terminator=:
bird1:goose
:geese
mammal1:moose
bird1:
birdmore:geese
mammalmore:moose
mammal1:

> CREATE SOURCE nullkey
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-nullkey-${testdrive.seed}'
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

> select * from nullkey
key           text
-------------------
birdmore      geese
mammalmore    moose

$ kafka-create-topic topic=realtimeavroavro partitions=1

# test multi-field avro key
$ set keyschema2={
    "type": "record",
    "name": "Key2",
    "fields": [
        {"name": "f3", "type": ["null", "string"]},
        {"name": "f4", "type": ["null", "string"]}
    ]
  }

$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": {"string": "fire"}, "f4": {"string": "yang"}} {"f1": "dog", "f2": 42}
{"f3": null, "f4": {"string": "yin"}} {"f1": "sheep", "f2": 53}
{"f3": {"string": "water"}, "f4": null} {"f1":"plesiosaur", "f2": 224}
{"f3": {"string": "earth"}, "f4": {"string": "dao"}} {"f1": "turtle", "f2": 34}
{"f3": null, "f4": {"string": "yin"}} {"f1": "sheep", "f2": 54}
{"f3": {"string": "earth"}, "f4": {"string": "dao"}} {"f1": "snake", "f2": 68}
{"f3": {"string": "water"}, "f4": null} {"f1": "crocodile", "f2": 7}
{"f3": {"string": "earth"}, "f4":{"string": "dao"}}

> CREATE SOURCE realtimeavroavro (f3, f4, f1, f2)
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-realtimeavroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE MATERIALIZED VIEW realtimeavroavro_view as SELECT * from realtimeavroavro;

> select f3, f4, f1, f2 from realtimeavroavro_view
f3        f4      f1             f2
-----------------------------------
fire      yang    dog            42
<null>    yin     sheep          54
water     <null>  crocodile      7

# Ensure that Upsert sources work with START OFFSET
> CREATE SOURCE realtimeavroavro_ff
  FROM KAFKA CONNECTION kafka_conn
  WITH (START OFFSET=[1])
  TOPIC 'testdrive-realtimeavroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT * FROM realtimeavroavro_ff
f3        f4        f1           f2
-----------------------------------
<null>    yin       sheep        54
water     <null>    crocodile    7

# ensure that having deletion on a key that never existed does not break anything
$ kafka-ingest format=avro topic=realtimeavroavro key-format=avro key-schema=${keyschema2} schema=${schema} publish=true
{"f3": {"string": "fire"}, "f4": {"string": "yin"}}
{"f3": {"string": "air"}, "f4":{"string": "qi"}} {"f1": "pigeon", "f2": 10}
{"f3": {"string": "air"}, "f4":{"string": "qi"}} {"f1": "owl", "f2": 15}
{"f3": {"string": "earth"}, "f4": {"string": "dao"}} {"f1": "rhinoceros", "f2": 211}
{"f3": {"string": "air"}, "f4":{"string": "qi"}} {"f1": "chicken", "f2": 47}
{"f3": null, "f4":{"string": "yin"}}
{"f3": null, "f4":{"string": "yin"}} {"f1":"dog", "f2": 243}
{"f3": {"string": "water"}, "f4": null}

> select * from realtimeavroavro_view
f3         f4          f1             f2
-----------------------------------------
fire       yang        dog            42
air        qi          chicken        47
<null>     yin         dog            243
earth      dao         rhinoceros     211

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
  FROM KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-realtimefilteravro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
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

> CREATE SOURCE include_metadata
  FROM KAFKA CONNECTION kafka_conn
  WITH (START OFFSET=[1])
  TOPIC 'testdrive-realtimeavroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET
  ENVELOPE UPSERT

> SELECT * FROM include_metadata
f3        f4        f1           f2   partition  offset
-------------------------------------------------------
<null>    yin       dog          243   0         15
air       qi        chicken      47    0         13
earth     dao       rhinoceros   211   0         12

> CREATE SOURCE include_metadata_ts
  FROM KAFKA CONNECTION kafka_conn
  WITH (START OFFSET=[1])
  TOPIC 'testdrive-realtimeavroavro-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET, TIMESTAMP as ts
  ENVELOPE UPSERT

> SELECT "offset" FROM include_metadata_ts WHERE ts > '2021-01-01'
offset
------
15
13
12

> SELECT "offset" FROM include_metadata_ts WHERE ts < '2021-01-01'
offset
------
