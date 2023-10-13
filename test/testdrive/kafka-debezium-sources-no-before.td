# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that we can (and continue to do so) ingest debezium topics that don't
# have a `before` field in their schema.

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
      { "name": "op", "type": "string" },
      {
        "name": "after",
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
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=dbz-no-before partitions=1

# Note: we ignore the `op` field, so can be "u" or "c"

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "c", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"after": {"row": {"id": 1, "creature": "salamander"}}, "op": "c", "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"after": {"row": {"id": 1, "creature": "lizard"}}, "op": "c", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> CREATE SOURCE dbz_no_before
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-no-before-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * FROM dbz_no_before
id creature
-----------
1  lizard

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=2
{"id": 1} {"after": {"row": {"id": 1, "creature": "dino"}}, "op": "u", "source": {"file": "binlog4", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM dbz_no_before
id creature
-----------
1  dino

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=3
{"id": 2} {"after": {"row": {"id": 2, "creature": "archeopteryx"}}, "op": "c", "source": {"file": "binlog5", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 2} {"after": {"row": {"id": 2, "creature": "velociraptor"}}, "op": "u", "source": {"file": "binlog6", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM dbz_no_before ORDER BY creature
id creature
------------
1  dino
2  velociraptor

# test duplicates
$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=4
{"id": 3} {"after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog7", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 3} {"after": {"row": {"id": 3, "creature": "triceratops"}}, "op": "u", "source": {"file": "binlog8", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM dbz_no_before WHERE id = 3
id creature
-----------
3  triceratops

# test removal and reinsertion

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=5
{"id": 4} {"after": {"row": {"id": 4, "creature": "moros"}}, "op": "c", "source": {"file": "binlog9", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM dbz_no_before WHERE id = 4
creature
--------
moros

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=6
{"id": 4} {"after": null, "op": "d", "source": {"file": "binlog10", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM dbz_no_before WHERE id = 4
creature
--------

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=7
{"id": 4} {"after": {"row": {"id": 4, "creature": "chicken"}}, "op": "u", "source": {"file": "binlog11", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT creature FROM dbz_no_before WHERE id = 4
creature
--------
chicken

> SELECT * FROM dbz_no_before WHERE id = 3
id creature
-----------
3  triceratops
