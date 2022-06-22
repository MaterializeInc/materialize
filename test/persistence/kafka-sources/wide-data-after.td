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


# Cause some more rows to be produced in the Kafka topic

$ kafka-ingest format=avro topic=wide-data-ten key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10 start-iteration=10
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> SELECT COUNT(*), MIN(LENGTH(value)), MAX(LENGTH(value)) FROM wide_data_source;
20 524288 524288
