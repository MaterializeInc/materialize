# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Insert 10K values prior to restart and check that we never re-ingest
# the entire topic as observed by the librdkafka counters
#

$ set count=10000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=delayed-materialization

$ kafka-ingest format=avro topic=delayed-materialization key-format=avro key-schema=${keyschema} schema=${schema} repeat=1
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE materialized_via_create_index_before_restart
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-delayed-materialization-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE SOURCE materialized_via_create_materialized_view_before_restart
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-delayed-materialization-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE SOURCE materialized_via_create_index_after_restart
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-delayed-materialization-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE SOURCE materialized_via_create_materialized_view_after_restart
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-delayed-materialization-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

$ kafka-ingest format=avro topic=delayed-materialization key-format=avro key-schema=${keyschema} schema=${schema} repeat=${count}
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

> CREATE DEFAULT INDEX ON materialized_via_create_index_before_restart;

> SELECT COUNT(*) FROM materialized_via_create_index_before_restart;
"${count}"

> CREATE MATERIALIZED VIEW materialized_via_create_materialized_view_before_restart_view AS SELECT COUNT(*) AS f1 FROM materialized_via_create_materialized_view_before_restart;
