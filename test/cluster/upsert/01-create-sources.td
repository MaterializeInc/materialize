# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The main purpose of these tests is to make sure that we can correctly retract
# key/value errors (mostly decoding errors) even after we have restarted a
# materialize instance.

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

$ kafka-create-topic topic=dbzupsert-broken-key partitions=1

$ kafka-ingest format=avro topic=dbzupsert-broken-key key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "lizard"}}}

$ kafka-create-topic topic=dbzupsert-broken-value partitions=1

$ kafka-ingest format=avro topic=dbzupsert-broken-value key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "lizard"}}}

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# With these first two sources, we verify that we can retract key/value decoding errors.

> CREATE SOURCE upsert_broken_key
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-broken-key-${testdrive.seed}')

> CREATE TABLE upsert_broken_key_tbl FROM SOURCE upsert_broken_key (REFERENCE "testdrive-dbzupsert-broken-key-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE SOURCE upsert_broken_value
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-broken-value-${testdrive.seed}')

> CREATE TABLE upsert_broken_value_tbl FROM SOURCE upsert_broken_value (REFERENCE "testdrive-dbzupsert-broken-value-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * FROM upsert_broken_key_tbl
id creature
-----------
1  lizard

# Ingest a broken key/value pair
$ kafka-ingest format=bytes topic=dbzupsert-broken-key key-format=bytes
broken-key:bar

! SELECT * FROM upsert_broken_key_tbl
contains: Key decode

# Ingest a broken value with a good key
$ kafka-ingest format=bytes topic=dbzupsert-broken-value key-format=avro key-schema=${keyschema}
{"id": 2}bar2

! SELECT * FROM upsert_broken_value_tbl
contains: Value error

# With this third source, we verify that we can retract NULL-key errors by
# ingesting a NULL:NULL record (a record where both key and value are NULL).

$ kafka-create-topic topic=upsert-nullkey partitions=1

# A null key should result in an error decoding that row but not a panic
$ kafka-ingest format=bytes topic=upsert-nullkey key-format=bytes key-terminator=:
bird1:goose
:geese

> CREATE SOURCE upsert_nullkey
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-nullkey-${testdrive.seed}')

> CREATE TABLE upsert_nullkey_tbl FROM SOURCE upsert_nullkey (REFERENCE "testdrive-upsert-nullkey-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

! select * from upsert_nullkey_tbl
contains: record with NULL key in UPSERT source
