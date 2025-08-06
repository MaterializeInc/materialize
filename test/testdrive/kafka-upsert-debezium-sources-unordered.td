# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# must be a subset of the keys in the rows
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
                "name": "creature",
                "type": "string"
              },
              {
                  "name": "id",
                  "type": "long"
              }]
           },
           "null"
         ]
      },
      {
        "name": "after",
        "type": ["row", "null"]
      }
    ]
  }

$ kafka-create-topic topic=dbzupsert

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"creature": "fish", "id": 1}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": {"row": {"creature": "mudskipper", "id": 1}}, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": {"row": {"creature": "salamander", "id": 1}}, "after": {"row": {"id": 1, "creature": "lizard"}}}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE doin_upsert
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')

> CREATE TABLE doin_upsert_tbl FROM SOURCE doin_upsert (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT * FROM doin_upsert_tbl
creature id
-----------
lizard   1
