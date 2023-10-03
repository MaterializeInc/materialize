# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that we still see the non-legacy errors in the shard

! SELECT * FROM textavro
exact: Envelope error: Upsert: Value error: Text: avro deserialization error: wrong Confluent-style avro serialization magic: expected 0, got 32 (original bytes: [20, 67, 61, 72, 62, 61, 67, 65]), decoded key: Row{[String("fish")]}

# Retract the error from the topic and check that the source returns to a working state

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-ingest format=avro topic=textavro key-format=bytes key-terminator=: schema=${schema}
fish: {"f1": "fish", "f2": 1000}

> SELECT * FROM textavro
fish fish 1000
