# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a test that was used to ensure a specific ordering of updates
# in `upsert` continued to be processed properly in pr materialize#24663. Its short,
# so its copied here.

$ set-arg-default default-storage-size=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

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
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=dbz-no-before partitions=1

# Note: we ignore the `op` field, so can be "u" or "c"

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "c", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"after": {"row": {"id": 1, "creature": "salamander"}}, "op": "c", "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}
{"id": 1} {"after": null, "op": "c", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> CREATE CLUSTER dbz_no_before_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE dbz_no_before
  IN CLUSTER dbz_no_before_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbz-no-before-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT count(*) FROM dbz_no_before
0

# WIP: The feedback upsert implementation does not count tombstones in
# bytes_indexed. For now.
> SELECT
    bool_and(u.snapshot_committed),
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('dbz_no_before')
  GROUP BY s.name
  ORDER BY s.name
true ${arg.leaves-tombstones} 0

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"after": {"row": {"id": 1, "creature": "mudskipper"}}, "op": "c", "source": {"file": "binlog1", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT * FROM dbz_no_before
id creature
-------------
1  mudskipper

> SELECT
    bool_and(u.snapshot_committed),
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('dbz_no_before')
  GROUP BY s.name
  ORDER BY s.name
true true 1

$ kafka-ingest format=avro topic=dbz-no-before key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"after": null, "op": "c", "source": {"file": "binlog3", "pos": 1, "row": 1, "snapshot": {"boolean": false}}}

> SELECT count(*) FROM dbz_no_before
0

# WIP: The feedback upsert implementation does not count tombstones in
# bytes_indexed. For now.
> SELECT
    bool_and(u.snapshot_committed),
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('dbz_no_before')
  GROUP BY s.name
  ORDER BY s.name
true ${arg.leaves-tombstones} 0
