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
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-ingest format=avro topic=partition-change key-format=avro key-schema=${keyschema} schema=${schema} start-iteration=10000 repeat=10000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> SELECT COUNT(*), MIN(f1), MAX(f1), MIN(f2), MAX(f2) FROM partition_change;
20000 0 19999 0 19999
