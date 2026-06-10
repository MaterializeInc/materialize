# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

# Must be a subset of the keys in the rows AND
# in a different order than the value.
$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "b", "type": "string"},
        {"name": "a", "type": "long"}
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
                  "name": "a",
                  "type": "long"
              },
              {
                "name": "data",
                "type": "string"
              },
              {
                  "name": "b",
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


> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=upsert-compound-key

$ kafka-ingest format=avro topic=upsert-compound-key key-format=avro key-schema=${keyschema} schema=${schema}
{"b": "bdata", "a": 1} {"before": null, "after": {"row": {"a": 1, "data": "fish1", "b": "bdata"}}}

> CREATE CLUSTER upsert_cluster SIZE = 'scale=1,workers=1', REPLICATION FACTOR = 1;

> CREATE SOURCE upsert_compound_key
  IN CLUSTER upsert_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-compound-key-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# Ingest the snapshot
> SELECT * FROM upsert_compound_key
1 fish1 bdata


# Stop the dataflow
> ALTER CLUSTER upsert_cluster SET (REPLICATION FACTOR 0);

# Ingest another update
$ kafka-ingest format=avro topic=upsert-compound-key key-format=avro key-schema=${keyschema} schema=${schema}
{"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish1", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish2", "b": "bdata"}}}

# Start the dataflow
> ALTER CLUSTER upsert_cluster SET (REPLICATION FACTOR 1);

# Verify result
> SELECT * FROM upsert_compound_key
1 fish2 bdata
