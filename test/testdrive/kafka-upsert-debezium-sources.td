# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "u", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}, "op": "u", "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}, "op": "u", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}


# Test that `WITH (start_offset=<whatever>)` gives an error on SELECT if misconfigured
> CREATE MATERIALIZED SOURCE start_offset_misconfigured
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (start_offset = 1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

! SELECT * FROM start_offset_misconfigured
contains:Invalid data in source, saw retractions (1) for row that does not exist

! CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM UPSERT
contains:ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

> CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert
id creature
-----------
1  lizard

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=2
{"id": 1} {"before": {"row": {"id": 1, "creature": "lizard"}}, "after": {"row": {"id": 1, "creature": "dino"}}, "op": "u", "source": {"file": "binlog4", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert
id creature
-----------
1  dino

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=3
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "archeopteryx"}}, "op": "c", "source": {"file": "binlog5", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 2} {"before": {"row": {"id": 2, "creature": "archeopteryx"}}, "after": {"row": {"id": 2, "creature": "velociraptor"}}, "op": "u", "source": {"file": "binlog6", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert ORDER BY creature
id creature
------------
1  dino
2  velociraptor

# test duplicates
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=4
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog7", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog8", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert WHERE id = 3
id creature
-----------
3  triceratops

# test removal and reinsertion
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=5
{"id": 4} {"before": null, "after": {"row": {"id": 4, "creature": "moros"}}, "op": "c", "source": {"file": "binlog9", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------
moros

! CREATE MATERIALIZED SOURCE doin_upsert_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE OFFSET
  ENVELOPE DEBEZIUM
contains:INCLUDE OFFSET with Debezium requires UPSERT semantics

> CREATE MATERIALIZED SOURCE doin_upsert_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE PARTITION, OFFSET AS test_kafka_offset
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert_metadata WHERE id = 4
id creature partition test_kafka_offset
---------------------------------------
4  moros    0         9

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=6
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": null, "op": "d", "source": {"file": "binlog10", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=7
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": {"row": {"id": 4, "creature": "chicken"}}, "op": "u", "source": {"file": "binlog11", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------
chicken

> SELECT * FROM doin_upsert WHERE id = 3
id creature
-----------
3  triceratops

# Test that `WITH (start_offset=<whatever>)` works
> CREATE MATERIALIZED SOURCE upsert_fast_forward
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (start_offset = 6)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_fast_forward WHERE id = 3
id creature
-----------
3  triceratops

# Test that it doesn't work with full deduplication
! CREATE MATERIALIZED SOURCE upsert_full_dedupe
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT
contains:unexpected parameters for CREATE SOURCE: deduplication


# test include metadata
> CREATE MATERIALIZED SOURCE upsert_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE OFFSET, PARTITION
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_metadata
id   creature      offset  partition
------------------------------------
1    dino          4       0
2    velociraptor  6       0
3    triceratops   8       0
4    chicken       11      0

# test include metadata respects metadata order
> CREATE MATERIALIZED SOURCE upsert_metadata_reordered
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE PARTITION, OFFSET
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_metadata_reordered
id   creature      partition  offset
------------------------------------
1    dino          0          4
2    velociraptor  0          6
3    triceratops   0          8
4    chicken       0          11
