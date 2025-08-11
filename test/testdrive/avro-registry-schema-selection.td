# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

# Tests that precisely manipulate which schema to select from the
# Confluent Schema Registry.

$ kafka-create-topic topic=schema-strategy-test

$ set first-writer-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}]}
$ set second-writer-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "long"}, {"name": "c", "type": ["null", "long"], "default": null}]}
$ set reader-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}]}

$ kafka-ingest format=avro topic=schema-strategy-test schema=${first-writer-schema} set-schema-id-var=id1
{"a": 0, "b": 1}

$ kafka-ingest format=avro topic=schema-strategy-test schema=${second-writer-schema} set-schema-id-var=id2
{"a": 2, "b": 3, "c": {"long": 4}}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER schema_strategy_test_inline_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE schema_strategy_test_inline
  IN CLUSTER schema_strategy_test_inline_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}')

> CREATE TABLE schema_strategy_test_inline_tbl FROM SOURCE schema_strategy_test_inline (REFERENCE "testdrive-schema-strategy-test-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE STRATEGY INLINE '${reader-schema}'
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_inline_tbl
a
---
0
2

> CREATE CLUSTER schema_strategy_test_id_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE schema_strategy_test_id
  IN CLUSTER schema_strategy_test_id_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}')

> CREATE TABLE schema_strategy_test_id_tbl FROM SOURCE schema_strategy_test_id (REFERENCE "testdrive-schema-strategy-test-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE STRATEGY ID ${id1}
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_id_tbl
a b
---
0 1
2 3

> CREATE CLUSTER schema_strategy_test_id2_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE schema_strategy_test_id2
  IN CLUSTER schema_strategy_test_id2_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}')

> CREATE TABLE schema_strategy_test_id2_tbl FROM SOURCE schema_strategy_test_id2 (REFERENCE "testdrive-schema-strategy-test-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE STRATEGY ID ${id2}
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_id2_tbl
a b c
-----
0 1 <null>
2 3 4

> CREATE CLUSTER schema_strategy_test_latest_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE schema_strategy_test_latest
  IN CLUSTER schema_strategy_test_latest_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-schema-strategy-test-${testdrive.seed}')

> CREATE TABLE schema_strategy_test_latest_tbl FROM SOURCE schema_strategy_test_latest (REFERENCE "testdrive-schema-strategy-test-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE STRATEGY LATEST
  ENVELOPE NONE

> SELECT * FROM schema_strategy_test_latest_tbl
a b c
-----
0 1 <null>
2 3 4
