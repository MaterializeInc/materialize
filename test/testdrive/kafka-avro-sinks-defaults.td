# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro UPSERT sinks.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_envelope_materialize = true



# sinking directly from an UPSERT source with multi-part key

$ set upsert-keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key1", "type": "string"},
        {"name": "key2", "type": ["null", "long"]}
    ]
  }

$ set upsert-schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":["null", "string"]},
            {"name":"f2", "type":["long", "null"]}
        ]
    }

$ kafka-create-topic topic=upsert-avro

$ kafka-ingest format=avro topic=upsert-avro key-format=avro key-schema=${upsert-keyschema} schema=${upsert-schema}
{"key1": "fish", "key2": {"long": 2}} {"f1": {"string": "fish"}, "f2": {"long": 1000}}
{"key1": "fisch", "key2": {"long": 42}} {"f1": null, "f2": {"long": 1000}}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE upsert_input
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-avro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT * FROM upsert_input;
fisch  42  <null>   1000
fish    2  fish     1000

# Checking all combination of NULL DEFAULTS with and without values

> CREATE SINK upsert_input_sink1 FROM upsert_input
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-input-sink1-${testdrive.seed}')
  KEY (key1, key2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS TRUE
  )
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-upsert-input-sink1-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"key1","type":"string"},{"name":"key2","type":["null","long"],"default":"null"},{"name":"f1","type":["null","string"],"default":"null"},{"name":"f2","type":["null","long"],"default":"null"}]}


> CREATE SINK upsert_input_sink2 FROM upsert_input
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-input-sink2-${testdrive.seed}')
  KEY (key1, key2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS
  )
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-upsert-input-sink2-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"key1","type":"string"},{"name":"key2","type":["null","long"],"default":"null"},{"name":"f1","type":["null","string"],"default":"null"},{"name":"f2","type":["null","long"],"default":"null"}]}


> CREATE SINK upsert_input_sink3 FROM upsert_input
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-input-sink3-${testdrive.seed}')
  KEY (key1, key2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS FALSE
  )
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-upsert-input-sink3-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"key1","type":"string"},{"name":"key2","type":["null","long"]},{"name":"f1","type":["null","string"]},{"name":"f2","type":["null","long"]}]}

> CREATE SINK upsert_input_sink4 FROM upsert_input
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-input-sink4-${testdrive.seed}')
  KEY (key1, key2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-upsert-input-sink4-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"key1","type":"string"},{"name":"key2","type":["null","long"]},{"name":"f1","type":["null","string"]},{"name":"f2","type":["null","long"]}]}

# Different types of columns
> CREATE TYPE point AS (x integer, y integer);

> CREATE TABLE t (c1 point, c2 text NOT NULL);
> INSERT INTO t SELECT ROW(1, 1)::point AS c1, 'text' AS c2;

> CREATE MATERIALIZED VIEW v AS SELECT * from t;

> CREATE SINK sink1 FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS
  )
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","fields":[{"name":"x","type":["null","int"],"default":"null"},{"name":"y","type":["null","int"],"default":"null"}]}],"default":"null"},{"name":"c2","type":"string"}]}

> CREATE SINK sink2 FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  KEY (c2) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  (
    NULL DEFAULTS FALSE
  )
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-sink1-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"c1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","fields":[{"name":"x","type":["null","int"]},{"name":"y","type":["null","int"]}]}]},{"name":"c2","type":"string"}]}
