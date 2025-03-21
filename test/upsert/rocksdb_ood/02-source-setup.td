# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ kafka-create-topic topic=upsert

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "eaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "faaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "gaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "haaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "iaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "jaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "kaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "maaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}
{"key": "naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {"f1": "11111111111111111111111111111111111111111111111111111111111111111111111111111111", "f2": 1000}

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT

> CREATE CONNECTION c_conn
  FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}'

> CREATE SOURCE upsert
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE upsert_tbl FROM SOURCE upsert (REFERENCE "testdrive-upsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION c_conn
  ENVELOPE UPSERT

> SELECT * from upsert_tbl
key           f1       f2
---------------------------
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
eaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
faaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
gaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
haaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
iaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
jaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
kaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
maaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 11111111111111111111111111111111111111111111111111111111111111111111111111111111 1000
