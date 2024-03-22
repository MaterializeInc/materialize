# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "id", "type": "long"}
    ]
  }

$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=weird

$ kafka-ingest format=avro key-format=avro topic=weird key-schema=${keyschema} schema=${schema}
{"id": 1} {"id": 2, "b": 3}

$ kafka-ingest format=avro key-format=bytes topic=weird schema=${schema} key-terminator=:
weird:{"id": 2, "b": 3}

$ kafka-ingest format=avro topic=weird schema=${schema} omit-key=true
{"id": 2, "b": 3}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE no_key_format
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-weird-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE KEY

> SHOW COLUMNS FROM no_key_format;
name  nullable  type
-----------------------
key   true     bytea
id    false    bigint
b     false    bigint

> SELECT key IS NOT NULL as key, id, b from no_key_format
key           id       b
------------------------
false        2        3
true         2        3
true         2        3

> CREATE SOURCE just_format
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-weird-${testdrive.seed}')
  FORMAT BYTES
  INCLUDE KEY AS key_col

> SHOW COLUMNS FROM just_format;
name     nullable  type
-----------------------
key_col  true     bytea
data     false    bytea

> SELECT key_col IS NOT NULL as key_col from just_format
key_col
-------
false
true
true

$ kafka-create-topic topic=weird-upsert
$ kafka-ingest format=bytes key-format=bytes topic=weird-upsert key-terminator=:
one:one

> CREATE SOURCE upsert_no_key_format
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-weird-upsert-${testdrive.seed}')
  FORMAT BYTES
  ENVELOPE UPSERT

> SHOW COLUMNS FROM upsert_no_key_format
name     nullable  type
-----------------------
key      false    bytea
data     false    bytea

> SELECT * from upsert_no_key_format
key    data
-----------
one    one

$ kafka-ingest format=bytes key-format=bytes topic=weird-upsert key-terminator=:
:one

! SELECT * from upsert_no_key_format
contains: Null key

$ kafka-ingest format=bytes key-format=bytes topic=weird-upsert key-terminator=:
:

> SELECT * from upsert_no_key_format
key    data
-----------
one    one
