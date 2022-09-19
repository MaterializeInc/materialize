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
        {"name": "f1", "type": "string"},
        {"name": "f2", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f3", "type":"string"},
            {"name":"f4", "type":"string"}
        ]
    }

> SELECT COUNT(*), COUNT(DISTINCT f2), COUNT(DISTINCT f4) FROM multipart_key WHERE f1 = 'KEY1';
10000 10000 10000

> SELECT COUNT(*), COUNT(DISTINCT f1), COUNT(DISTINCT f3) FROM multipart_key WHERE f2 = 'KEY2';
10000 10000 10000

# Delete all rows
$ kafka-ingest format=avro topic=multipart-key key-format=avro key-schema=${keyschema} schema=${schema} repeat=10000
{"f1": "KEY1", "f2": "${kafka-ingest.iteration}"}
{"f1": "${kafka-ingest.iteration}", "f2": "KEY2"}

> SELECT COUNT(*) FROM multipart_key;
0
