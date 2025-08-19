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
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=dbzupsert partitions=1

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "u", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}, "op": "u", "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}, "op": "u", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> CREATE SOURCE doin_upsert
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
! CREATE TABLE doin_upsert_tbl FROM SOURCE doin_upsert (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM
contains:ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified

! CREATE TABLE doin_upsert_tbl FROM SOURCE doin_upsert (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  KEY FORMAT JSON VALUE FORMAT JSON
  ENVELOPE DEBEZIUM
contains:ENVELOPE DEBEZIUM requires that VALUE FORMAT is set to AVRO

> CREATE TABLE doin_upsert_tbl FROM SOURCE doin_upsert (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * FROM doin_upsert_tbl
id creature
-----------
1  lizard

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=2
{"id": 1} {"before": {"row": {"id": 1, "creature": "lizard"}}, "after": {"row": {"id": 1, "creature": "dino"}}, "op": "u", "source": {"file": "binlog4", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert_tbl
id creature
-----------
1  dino

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=3
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "archeopteryx"}}, "op": "c", "source": {"file": "binlog5", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 2} {"before": {"row": {"id": 2, "creature": "archeopteryx"}}, "after": {"row": {"id": 2, "creature": "velociraptor"}}, "op": "u", "source": {"file": "binlog6", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert_tbl ORDER BY creature
id creature
------------
1  dino
2  velociraptor

# test duplicates
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=4
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog7", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 3} {"before": {"row": {"id": 3, "creature": "protoceratops"}}, "after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog8", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM doin_upsert_tbl WHERE id = 3
id creature
-----------
3  triceratops

# test removal and reinsertion
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=5
{"id": 4} {"before": null, "after": {"row": {"id": 4, "creature": "moros"}}, "op": "c", "source": {"file": "binlog9", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert_tbl WHERE id = 4
creature
--------
moros

> BEGIN
> CREATE SOURCE doin_upsert_metadata
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')

# [btv] uncomment if we bring back classic debezium mode
# ! CREATE TABLE doin_upsert_metadata_tbl FROM SOURCE doin_upsert_metadata (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
#   FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
#   INCLUDE OFFSET
#   ENVELOPE DEBEZIUM
# contains:INCLUDE OFFSET with Debezium requires UPSERT semantics

> CREATE TABLE doin_upsert_metadata_tbl FROM SOURCE doin_upsert_metadata (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET AS test_kafka_offset
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT * FROM doin_upsert_metadata_tbl WHERE id = 4
id creature partition test_kafka_offset
---------------------------------------
4  moros    0         8

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=6
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": null, "op": "d", "source": {"file": "binlog10", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert_tbl WHERE id = 4
creature
--------

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} timestamp=7
{"id": 4} {"before": {"row": {"id": 4, "creature": "trex"}}, "after": {"row": {"id": 4, "creature": "chicken"}}, "op": "u", "source": {"file": "binlog11", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM doin_upsert_tbl WHERE id = 4
creature
--------
chicken

> SELECT * FROM doin_upsert_tbl WHERE id = 3
id creature
-----------
3  triceratops

# Test that `WITH (START OFFSET=<whatever>)` works
> BEGIN
> CREATE SOURCE upsert_fast_forward
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (START OFFSET = [6], TOPIC 'testdrive-dbzupsert-${testdrive.seed}')

> CREATE TABLE upsert_fast_forward_tbl FROM SOURCE upsert_fast_forward (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT * FROM upsert_fast_forward_tbl WHERE id = 3
id creature
-----------
3  triceratops

# test include metadata
> BEGIN
> CREATE SOURCE upsert_metadata
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')

> CREATE TABLE upsert_metadata_tbl FROM SOURCE upsert_metadata (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE OFFSET, PARTITION
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT * FROM upsert_metadata_tbl
id   creature      offset  partition
------------------------------------
1    dino          3       0
2    velociraptor  5       0
3    triceratops   7       0
4    chicken       10      0

# test include metadata respects metadata order
> BEGIN
> CREATE SOURCE upsert_metadata_reordered
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')

> CREATE TABLE upsert_metadata_reordered_tbl FROM SOURCE upsert_metadata_reordered (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION, OFFSET
  ENVELOPE DEBEZIUM
> COMMIT

> SELECT * FROM upsert_metadata_reordered_tbl
id   creature      partition  offset
------------------------------------
1    dino          0          3
2    velociraptor  0          5
3    triceratops   0          7
4    chicken       0          10
