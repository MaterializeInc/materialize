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

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

$ kafka-create-topic topic=dbzupsert partitions=1

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "u", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}, "op": "u", "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}, "op": "u", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

! CREATE SOURCE doin_upsert
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM UPSERT
contains:ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

> CREATE SOURCE doin_upsert
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert
id creature
-----------
1  lizard

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=2
{"id": 1} {"before": {"row": {"id": 1, "creature": "lizard"}}, "after": {"row": {"id": 1, "creature": "dino"}}, "op": "u", "source": {"file": "binlog4", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert
id creature
-----------
1  dino

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=3
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "archeopteryx"}}, "op": "c", "source": {"file": "binlog5", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 2} {"before": {"row": {"id": 2, "creature": "archeopteryx"}}, "after": {"row": {"id": 2, "creature": "velociraptor"}}, "op": "u", "source": {"file": "binlog6", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert ORDER BY creature
id creature
------------
1  dino
2  velociraptor

# test duplicates
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=4
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog7", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog8", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert WHERE id = 3
id creature
-----------
3  triceratops

# test removal and reinsertion
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=5
{"id": 4} {"before": null, "after": {"row": {"id": 4, "creature": "moros"}}, "op": "c", "source": {"file": "binlog9", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------
moros

! CREATE SOURCE doin_upsert_metadata
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE OFFSET
  ENVELOPE DEBEZIUM
contains:INCLUDE OFFSET with Debezium requires UPSERT semantics

> CREATE SOURCE doin_upsert_metadata
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET AS test_kafka_offset
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM doin_upsert_metadata WHERE id = 4
id creature partition test_kafka_offset
---------------------------------------
4  moros    0         9

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=6
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": null, "op": "d", "source": {"file": "binlog10", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=7
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": {"row": {"id": 4, "creature": "chicken"}}, "op": "u", "source": {"file": "binlog11", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert WHERE id = 4
creature
--------
chicken

> SELECT * FROM doin_upsert WHERE id = 3
id creature
-----------
3  triceratops

# Test that `WITH (START OFFSET=<whatever>)` works
> CREATE SOURCE upsert_fast_forward
  FROM KAFKA CONNECTION kafka_conn (START OFFSET = [6], TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_fast_forward WHERE id = 3
id creature
-----------
3  triceratops

# Test that it doesn't work with full deduplication
! CREATE SOURCE upsert_full_dedupe
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  LEGACYWITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM UPSERT
contains:unexpected parameters for CREATE SOURCE: deduplication


# test include metadata
> CREATE SOURCE upsert_metadata
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
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
> CREATE SOURCE upsert_metadata_reordered
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET
  ENVELOPE DEBEZIUM UPSERT

> SELECT * FROM upsert_metadata_reordered
id   creature      partition  offset
------------------------------------
1    dino          0          4
2    velociraptor  0          6
3    triceratops   0          8
4    chicken       0          11
