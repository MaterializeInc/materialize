# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic connector functionality

###
# Test core functionality by creating, introspecting and dropping a connector
###
$ kafka-create-topic topic=connector_test partitions=1
$ kafka-ingest format=bytes topic=connector_test
1,2
2,3

> CREATE CONNECTOR testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> SELECT name, connector_type from mz_connectors
name  connector_type
------------------------------
testconn   kafka

> SHOW CREATE CONNECTOR testconn
Connector   "Create Connector"
---------------------------------
materialize.public.testconn   "CREATE CONNECTOR \"materialize\".\"public\".\"testconn\" FOR KAFKA BROKER '${testdrive.kafka-addr}'"


> DROP CONNECTOR testconn

###
# Test that connectors work in creating a source
###
> CREATE CONNECTOR testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE MATERIALIZED SOURCE connector_source (first, second)
  FROM KAFKA CONNECTOR testconn
  TOPIC 'testdrive-connector_test-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM connector_source
first second mz_offset
----------------------
1     2      1
2     3      2

# Confirm we cannot drop the connector while a source depends upon it
! DROP CONNECTOR testconn;
contains:depended upon by catalog item 'materialize.public.connector_source'

# Confirm the drop works if we add cascade
> DROP CONNECTOR testconn CASCADE;

# Validate the cascading drop actually happened
! SELECT * FROM connector_source
contains:unknown catalog item 'connector_source'

###
# Test schema registry connector create and drop
###

# Setup kafka topic with schema
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

$ kafka-create-topic topic=csr_test partitions=1

$ kafka-ingest format=avro topic=csr_test key-format=avro key-schema=${keyschema} schema=${schema} publish=true timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}}



> CREATE CONNECTOR csr_conn
  FOR CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED SOURCE csr_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-csr_test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTOR csr_conn
  ENVELOPE DEBEZIUM UPSERT

> SELECT * from csr_source
id creature
-----------
1  lizard


> CREATE CONNECTOR broker_connector
  FOR KAFKA BROKER '${testdrive.kafka-addr}'


> CREATE MATERIALIZED SOURCE two_connector_source
  FROM KAFKA CONNECTOR broker_connector
  TOPIC 'testdrive-csr_test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTOR csr_conn
  ENVELOPE DEBEZIUM UPSERT

> SELECT * from two_connector_source
id creature
-----------
1  lizard

! DROP CONNECTOR csr_conn
contains:depended upon by catalog item 'materialize.public.csr_source'

> DROP CONNECTOR csr_conn CASCADE

! CREATE MATERIALIZED SOURCE should_fail
  FROM KAFKA CONNECTOR does_not_exist
  TOPIC 'error_topic'
  FORMAT TEXT
contains: unknown catalog item 'does_not_exist'

! CREATE MATERIALIZED SOURCE should_fail
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-csr_test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTOR does_not_exist
  ENVELOPE DEBEZIUM UPSERT
contains: unknown catalog item 'does_not_exist'
