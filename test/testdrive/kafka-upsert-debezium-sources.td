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
      {
        "name": "after",
        "type": ["row", "null"]
      }
    ]
  }

$ kafka-create-topic topic=dbzupsert

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}}


# Test that `WITH (start_offset=<whatever>)` gives an error on SELECT if misconfigured
> CREATE MATERIALIZED SOURCE start_offset_misconfigured
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (start_offset = 1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

! SELECT * FROM start_offset_misconfigured
Updates and deletes are not allowed for this source! This probably means it was started with `start_offset` and without `UPSERT` semantics.

! CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM UPSERT
ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

> CREATE MATERIALIZED SOURCE doin_upsert
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert
id creature
-----------
1  lizard

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=2
{"id": 1} {"before": {"row": {"id": 1, "creature": "lizard"}}, "after": {"row": {"id": 1, "creature": "dino"}}}

> SELECT * FROM doin_upsert
id creature
-----------
1  dino

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=3
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "archeopteryx"}}}
{"id": 2} {"before": {"row": {"id": 2, "creature": "archeopteryx"}}, "after": {"row": {"id": 2, "creature": "velociraptor"}}}

> SELECT * FROM doin_upsert ORDER BY creature
id creature
------------
1  dino
2  velociraptor

# test duplicates
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=4
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}}
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}}

> SELECT * FROM doin_upsert WHERE id = 3
id creature
-----------
3  triceratops

# test removal and reinsertion
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=5
{"id": 4} {"before": null, "after": {"row": {"id": 4, "creature": "moros"}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------
moros

! CREATE MATERIALIZED SOURCE doin_upsert_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE OFFSET
  ENVELOPE DEBEZIUM
INCLUDE OFFSET with Debezium requires UPSERT semantics

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
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": null}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=7
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": {"row": {"id": 4, "creature": "chicken"}}}

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

# Test that `WITH (kafka_time_offset=<whatever>)` works
> CREATE MATERIALIZED SOURCE upsert_time_skip
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (kafka_time_offset = 6)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_time_skip
id creature
-----------
4  chicken

# Test that it doesn't work with full deduplication
! CREATE MATERIALIZED SOURCE upsert_full_dedupe
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dbzupsert-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM UPSERT
Debezium deduplication does not make sense with upsert sources


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
