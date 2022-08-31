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

$ kafka-ingest format=avro topic=exactly-once key-format=avro key-schema=${keyschema} schema=${schema} repeat=2 start-iteration=40
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}


> SELECT COUNT(*) FROM exactly_once;
10

# We expect just the 6 new messages, having consumed the older ones in xactly-once-sink-before.td
$ set-regex match=\d{13} replacement=<TIMESTAMP>
$ kafka-verify format=avro sink=materialize.public.exactly_once_sink sort-messages=true
{"before":null,"after":{"row":{"f1":20,"f2":20}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":21,"f2":21}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":30,"f2":30}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":31,"f2":31}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":40,"f2":40}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":41,"f2":41}},"transaction":{"id":"<TIMESTAMP>"}}
