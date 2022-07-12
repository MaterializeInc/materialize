# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "foo",
    "fields": [
      {"name": "a", "type": "int"}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} publish=true
{"a": 1}

> CREATE SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> SELECT * FROM data
1
