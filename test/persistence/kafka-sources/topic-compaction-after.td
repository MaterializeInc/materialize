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
            {"name":"f2", "type":"string"}
        ]
    }

> SELECT COUNT(*) FROM topic_compaction WHERE f2 LIKE 'C%';
1000

$ kafka-ingest format=avro topic=topic-compaction key-format=avro key-schema=${keyschema} schema=${schema} repeat=1000
{"f1": ${kafka-ingest.iteration}} {"f2": "D${kafka-ingest.iteration}"}

> SELECT COUNT(*) FROM topic_compaction WHERE f2 LIKE 'D%';
1000
