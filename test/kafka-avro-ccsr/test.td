# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# See mzcompose.py for explanation

$ kafka-create-topic topic=schema-strategy-test

$ set first-writer-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}
$ set second-writer-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}, {"name": "c", "type": ["null", "long"], "default": null}]}
$ set reader-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}]}

$ kafka-ingest format=avro topic=schema-strategy-test schema=${first-writer-schema} timestamp=1 publish=true
{"a": 0, "b": 1}

$ kafka-ingest format=avro topic=schema-strategy-test schema=${second-writer-schema} timestamp=1 publish=true
{"a": 2, "b": 3, "c": {"long": 4}}

> CREATE MATERIALIZED SOURCE schema_strategy_test_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE STRATEGY INLINE '${reader-schema}'
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_inline
a
---
0
2

> CREATE MATERIALIZED SOURCE schema_strategy_test_id
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE STRATEGY ID 1
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_id
a b
---
0 1
2 3

> CREATE MATERIALIZED SOURCE schema_strategy_test_id2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE STRATEGY ID 2
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_id2
a b c
-----
0 1 <null>
2 3 4

> CREATE MATERIALIZED SOURCE schema_strategy_test_latest
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE STRATEGY LATEST
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_latest
a b c
-----
0 1 <null>
2 3 4
