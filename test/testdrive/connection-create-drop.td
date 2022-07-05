# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic connection functionality

###
# Test core functionality by creating, introspecting and dropping a connection
###
$ kafka-create-topic topic=connection_test partitions=1
$ kafka-ingest format=bytes topic=connection_test
1,2
2,3

> CREATE CONNECTION testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> SELECT name, type from mz_connections
name       type
------------------------------
testconn   kafka

> SHOW CREATE CONNECTION testconn
Connection   "Create Connection"
---------------------------------
materialize.public.testconn   "CREATE CONNECTION \"materialize\".\"public\".\"testconn\" FOR KAFKA BROKER = '${testdrive.kafka-addr}'"


> DROP CONNECTION testconn

###
# Test that connections work in creating a source
###
> CREATE CONNECTION testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE MATERIALIZED SOURCE connection_source (first, second)
  FROM KAFKA CONNECTION testconn
  TOPIC 'testdrive-connection_test-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM connection_source
first second
------------
1     2
2     3

# Confirm we cannot drop the connection while a source depends upon it
! DROP CONNECTION testconn;
contains:depended upon by catalog item 'materialize.public.connection_source'

# Confirm the drop works if we add cascade
> DROP CONNECTION testconn CASCADE;

# Validate the cascading drop actually happened
! SELECT * FROM connection_source
contains:unknown catalog item 'connection_source'

###
# Test schema registry connection create and drop
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


> CREATE CONNECTION csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED SOURCE csr_source
    FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-csr_test-${testdrive.seed}'
    FORMAT AVRO
    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM UPSERT

> SELECT * from csr_source
id creature
-----------
1  lizard


> CREATE CONNECTION broker_connection
  FOR KAFKA BROKER '${testdrive.kafka-addr}'


> CREATE MATERIALIZED SOURCE two_connection_source
  FROM KAFKA CONNECTION broker_connection
  TOPIC 'testdrive-csr_test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM UPSERT

> SELECT * from two_connection_source
id creature
-----------
1  lizard

! DROP CONNECTION csr_conn
contains:depended upon by catalog item 'materialize.public.csr_source'

> DROP CONNECTION csr_conn CASCADE

! CREATE MATERIALIZED SOURCE should_fail
  FROM KAFKA CONNECTION does_not_exist
  TOPIC 'error_topic'
  FORMAT TEXT
contains: unknown catalog item 'does_not_exist'

! CREATE MATERIALIZED SOURCE should_fail
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-csr_test-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION does_not_exist
  ENVELOPE DEBEZIUM UPSERT
contains: unknown catalog item 'does_not_exist'

# Test protobuf CSR connection
# Duplicated from protobuf-import.td since once a topic has been read we can only create the source again by forcing offsets which is itself a different test case
$ set empty-schema
syntax = "proto3";


$ set importee-schema
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Importee1 {
    bool b = 1;
}

message Importee2 {
    google.protobuf.Timestamp ts = 3;
}

$ set importer-schema
syntax = "proto3";

import "empty.proto";
import "importee.proto";

message Importer {
    Importee1 importee1 = 1;
    Importee2 importee2 = 2;
}

$ file-append path=empty.proto
\${empty-schema}

$ file-append path=importee.proto
\${importee-schema}

$ file-append path=importer.proto
\${importer-schema}

$ protobuf-compile-descriptors inputs=empty.proto,importee.proto,importer.proto output=import.pb

$ kafka-create-topic topic=import-csr partitions=1

# The Confluent toolchain publishes even schemas for well-known types, so we
# have to do the same.
# See: https://github.com/protocolbuffers/protobuf/blob/61e0395c89fe520ae7569aea6838313195e05ec5/src/google/protobuf/timestamp.proto
$ schema-registry-publish subject=google/protobuf/timestamp.proto schema-type=protobuf
syntax = "proto3";

package google.protobuf;

message Timestamp {
  int64 seconds = 1;
  int32 nanos = 2;
}

$ schema-registry-publish subject=empty.proto schema-type=protobuf
\${empty-schema}

$ schema-registry-publish subject=importee.proto schema-type=protobuf references=google/protobuf/timestamp.proto
\${importee-schema}

$ schema-registry-publish subject=testdrive-import-csr-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=Importer confluent-wire-format=true
{"importee1": {"b": false}, "importee2": {"ts": "1970-01-01T00:20:34.000005678Z"}}

> CREATE CONNECTION proto_csr
    FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED SOURCE import_connection_csr FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION proto_csr

> SELECT importee1::text, importee2::text FROM import_connection_csr
importee1  importee2
--------------------------------
(f)        "(\"(1234,5678)\")"

# Test invalid connection parameter combinations

## Kafka
! CREATE CONNECTION not_a_secret
  FOR KAFKA
    BROKER '',
    SASL PASSWORD = ''
contains:invalid SASL PASSWORD: must provide a secret value

! CREATE CONNECTION not_a_secret
  FOR KAFKA
    BROKER '',
    SSL KEY = ''
contains:invalid SSL KEY: must provide a secret value

! CREATE CONNECTION duplicate_option
  FOR KAFKA
    BROKER '',
    BROKER ''
contains:BROKER specified more than once

! CREATE CONNECTION no_broker
  FOR KAFKA
    SASL MECHANISMS = 'PLAIN';
contains:must set either BROKER or BROKERS

! CREATE CONNECTION ssl_underspeced
  FOR KAFKA
    BROKER 'kafka:9092',
    BROKERS ['kafka:9092', 'kafka:9093'];
contains:cannot set BROKER and BROKERS

! CREATE CONNECTION ssl_underspeced
  FOR KAFKA
    BROKER 'kafka:9092',
    SSL CERTIFICATE = '';
contains:under-specified security configuration

! CREATE CONNECTION sasl_underspeced
  FOR KAFKA
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'PLAIN';
contains:under-specified security configuration

! CREATE CONNECTION multiple_brokers
  FOR KAFKA
    BROKER 'kafka:9092, kafka:9093'
contains:cannot specify multiple Kafka broker addresses in one string

! CREATE CONNECTION multiple_brokers
  FOR KAFKA
    BROKERS ['kafka:9092, kafka:9093']
contains:cannot specify multiple Kafka broker addresses in one string

## CSR
! CREATE CONNECTION missing_url
    FOR CONFLUENT SCHEMA REGISTRY
    USERNAME 'foo'
contains: must specify URL

> CREATE SECRET s AS '...';

! CREATE CONNECTION missing_cert
    FOR CONFLUENT SCHEMA REGISTRY
    URL 'http://localhost',
    SSL KEY = SECRET s
contains: requires both SSL KEY and SSL CERTIFICATE

! CREATE CONNECTION missing_key
    FOR CONFLUENT SCHEMA REGISTRY
     URL 'http://localhost',
    SSL CERTIFICATE = ''
contains: requires both SSL KEY and SSL CERTIFICATE
