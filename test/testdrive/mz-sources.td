# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that envelope types are correctly reported in mz_sources

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=none-topic partitions=1

> CREATE SOURCE none_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-none-topic-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE NONE


$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "id", "type": "long"}
    ]
  }

$ set schema={
    "type" : "record",
    "name" : "envelope",
    "fields" : [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                  "name": "id",
                  "type": "long"
              },
              {
                "name": "creature",
                "type": "string"
              }]
           },
           "null"
         ]
      },
      { "name": "op", "type": "string" },
      {
        "name": "after",
        "type": ["row", "null"]
      },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "io.debezium.connector.mysql",
          "fields": [
            {
              "name": "file",
              "type": "string"
            },
            {
              "name": "pos",
              "type": "long"
            },
            {
              "name": "row",
              "type": "int"
            },
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "boolean",
                  "connect.default": false
                },
                "null"
              ],
              "default": false
            }
          ],
          "connect.name": "io.debezium.connector.mysql.Source"
        }
      }
    ]
  }


$ kafka-create-topic topic=dbzupsert partitions=1

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "u", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> CREATE SOURCE debezium_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-create-topic topic=upsert-topic

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-ingest format=avro topic=upsert-topic key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "fish", "f2": 1000}

> CREATE SOURCE upsert_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-topic-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT envelope_type FROM mz_sources WHERE name = 'none_source'
none

> SELECT envelope_type FROM mz_sources WHERE name = 'debezium_source'
debezium

> SELECT envelope_type FROM mz_sources WHERE name = 'upsert_source'
upsert

> SELECT subsources IS NULL FROM mz_sources WHERE name = 'upsert_source'
true
