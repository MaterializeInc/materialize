# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

# Verify that envelope types are correctly reported in mz_sources

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=none-topic partitions=1

> CREATE CLUSTER none_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE none_source
  IN CLUSTER none_source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-none-topic-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE NONE

> CREATE CLUSTER none_source_no_key_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE none_source_no_key
  IN CLUSTER none_source_no_key_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-none-topic-${testdrive.seed}')
  FORMAT TEXT
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

> CREATE CLUSTER debezium_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE debezium_source
  IN CLUSTER debezium_source_cluster
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

> CREATE CLUSTER upsert_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE upsert_source
  IN CLUSTER upsert_source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-topic-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> SELECT envelope_type FROM mz_sources WHERE name = 'none_source'
none

> SELECT envelope_type FROM mz_sources WHERE name = 'debezium_source'
debezium

> SELECT envelope_type FROM mz_sources WHERE name = 'upsert_source'
upsert

> SELECT key_format, value_format FROM mz_sources WHERE name = 'none_source'
text text

> SELECT key_format, value_format FROM mz_sources WHERE name = 'none_source_no_key'
<null> text

> SELECT key_format, value_format FROM mz_sources WHERE name = 'debezium_source'
avro avro
