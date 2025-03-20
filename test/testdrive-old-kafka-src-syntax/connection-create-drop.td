# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

#
# Test basic connection functionality

$ skip-consistency-checks reason="workflow uses SSH keys which we currently can't check"

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

###
# Test core functionality by creating, introspecting and dropping a connection
###
$ kafka-create-topic topic=connection_test partitions=1
$ kafka-ingest format=bytes topic=connection_test
1,2
2,3

> CREATE SECRET s AS '...';

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, ACCESS KEY ID = 'abc')
contains:KAFKA connections do not support ACCESS KEY ID values

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, ACCESS KEY ID = 'abc', PORT = 1)
contains:KAFKA connections do not support ACCESS KEY ID, PORT values

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL NOTHINGREAL)
contains: unknown security protocol: NOTHINGREAL

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SASL MECHANISMS 'PLAIN', SASL USERNAME 'materialize', SASL PASSWORD SECRET s, SSL KEY SECRET s)
contains: option SSL KEY not supported with this configuration

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, SASL USERNAME 'materialize')
contains: option SASL USERNAME not supported with this configuration

! CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, PROGRESS TOPIC REPLICATION FACTOR -4)
contains:PROGRESS TOPIC REPLICATION FACTOR must be greater than 0

> CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> SELECT name, type from mz_connections WHERE id LIKE 'u%'
name       type
------------------------------
testconn   kafka

> SHOW CONNECTIONS
testconn    kafka    ""

> SHOW CREATE CONNECTION testconn
name   create_sql
---------------------------------
materialize.public.testconn   "CREATE CONNECTION \"materialize\".\"public\".\"testconn\" TO KAFKA (BROKER = '${testdrive.kafka-addr}', SECURITY PROTOCOL = \"plaintext\")"

> SELECT
      brokers,
      sink_progress_topic = '_materialize-progress-' || mz_environment_id() || '-' || id
  FROM mz_kafka_connections
  JOIN mz_connections USING (id)
  WHERE name = 'testconn'
{${testdrive.kafka-addr}}   true

> DROP CONNECTION testconn

> CREATE CONNECTION progress_override TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    PROGRESS TOPIC 'override_topic',
    PROGRESS TOPIC REPLICATION FACTOR 1,
    SECURITY PROTOCOL PLAINTEXT
  )

> SELECT
    brokers, sink_progress_topic
  FROM mz_kafka_connections
  JOIN mz_connections USING (id)
  WHERE name = 'progress_override'
{${testdrive.kafka-addr}}   override_topic

###
# Test that connections work in creating a source
###
> CREATE CONNECTION testconn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CLUSTER connection_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE connection_source (first, second)
  IN CLUSTER connection_source_cluster
  FROM KAFKA CONNECTION testconn (TOPIC 'testdrive-connection_test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM connection_source
first second
------------
1     2
2     3

# Confirm we cannot drop the connection while a source depends upon it
! DROP CONNECTION testconn;
contains:depended upon by source "connection_source"

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

$ kafka-ingest format=avro topic=csr_test key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}}


> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );


! CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}',
    SESSION TOKEN = 'abc'
  );
contains:CONFLUENT SCHEMA REGISTRY connections do not support SESSION TOKEN values

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

! CREATE SOURCE csr_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION csr_conn (TOPIC 'testdrive-csr_test-${testdrive.seed}')
    FORMAT AVRO
    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM
contains:is not a KAFKA CONNECTION

> CREATE CLUSTER csr_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE csr_source
    IN CLUSTER csr_source_cluster
    FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-csr_test-${testdrive.seed}')
    FORMAT AVRO
    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM

> SELECT * from csr_source
id creature
-----------
1  lizard

> CREATE CONNECTION broker_connection TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CLUSTER two_connection_source_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE two_connection_source
  IN CLUSTER two_connection_source_cluster
  FROM KAFKA CONNECTION broker_connection (TOPIC 'testdrive-csr_test-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * from two_connection_source
id creature
-----------
1  lizard

! DROP CONNECTION csr_conn
contains:depended upon by source "csr_source"

> DROP CONNECTION csr_conn CASCADE

! CREATE SOURCE should_fail
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION does_not_exist (TOPIC 'testdrive-error_topic-${testdrive.seed}')
  FORMAT TEXT
contains: unknown catalog item 'does_not_exist'

! CREATE SOURCE should_fail
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn
  (TOPIC 'testdrive-csr_test-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION does_not_exist
  ENVELOPE DEBEZIUM
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

$ protobuf-compile-descriptors inputs=empty.proto,importee.proto,importer.proto output=import.pb set-var=import-schema

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

> CREATE CONNECTION proto_csr TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  )

> CREATE CLUSTER import_connection_csr_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE import_connection_csr
  IN CLUSTER import_connection_csr_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-import-csr-${testdrive.seed}')
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION proto_csr

> SELECT importee1::text, importee2::text FROM import_connection_csr
importee1  importee2
--------------------------------
(f)        "(\"(1234,5678)\")"

# SSH

! CREATE CONNECTION ssh_conn TO SSH TUNNEL (
    HOST 'host',
    USER 'user',
    PORT 1,
    REGION = 'abc'
  );
contains:SSH TUNNEL connections do not support REGION values

> CREATE CONNECTION ssh_conn TO SSH TUNNEL (
    HOST 'host',
    USER 'user',
    PORT 1
  );

> SELECT name, public_key_1 LIKE 'ssh-ed25519%' public_key_1
  FROM mz_ssh_tunnel_connections
  JOIN mz_connections USING (id)
name public_key_1
-----------
ssh_conn true

# Test invalid connection parameter combinations

## Kafka
! CREATE CONNECTION not_a_secret TO KAFKA (
    BROKER '',
    SASL PASSWORD = '',
    SECURITY PROTOCOL PLAINTEXT
  );
contains:invalid SASL PASSWORD: must provide a secret value

! CREATE CONNECTION not_a_secret TO KAFKA (
    BROKER '',
    SSL KEY = '',
    SECURITY PROTOCOL PLAINTEXT
  );
contains:invalid SSL KEY: must provide a secret value

! CREATE CONNECTION duplicate_option TO KAFKA (
    BROKER '',
    BROKER '',
    SECURITY PROTOCOL PLAINTEXT
  );
contains:BROKER specified more than once

! CREATE CONNECTION no_broker TO KAFKA (
    SECURITY PROTOCOL PLAINTEXT
  );
contains:must set one of BROKER, BROKERS, or AWS PRIVATELINK

! CREATE CONNECTION ssl_underspeced TO KAFKA (
    BROKER 'kafka:9092',
    BROKERS ['kafka:9092', 'kafka:9093'],
    SECURITY PROTOCOL PLAINTEXT
  );
contains:can only set one of BROKER, BROKERS, or AWS PRIVATELINK

! CREATE CONNECTION ssl_underspeced TO KAFKA (
    BROKER 'kafka:9092',
    SSL CERTIFICATE = ''
  );
contains:SSL KEY must be specified with SSL CERTIFICATE

! CREATE CONNECTION sasl_underspeced TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'PLAIN'
  );
contains:SASL USERNAME must be specified

> CREATE CONNECTION kafka_sasl_lowercase_string_mechanism TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'plain',
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET s
  ) WITH (VALIDATE = FALSE);
> CREATE CONNECTION kafka_sasl_spongebob_string_mechanism TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'pLaIN',
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET s
  ) WITH (VALIDATE = FALSE);
> CREATE CONNECTION kafka_sasl_uppercase_ident_mechanism TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = PLAIN,
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET s
  ) WITH (VALIDATE = FALSE);
> CREATE CONNECTION kafka_sasl_spongebob_ident_mechanism TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = pLaIN,
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET s
  ) WITH (VALIDATE = FALSE);

! CREATE CONNECTION multiple_brokers TO KAFKA (
    BROKER 'kafka:9092, kafka:9093',
    SECURITY PROTOCOL PLAINTEXT
  );
contains:cannot specify multiple Kafka broker addresses in one string

! CREATE CONNECTION multiple_brokers TO KAFKA (
    BROKERS ['kafka:9092, kafka:9093'],
    SECURITY PROTOCOL PLAINTEXT
  );
contains:cannot specify multiple Kafka broker addresses in one string

## CSR
! CREATE CONNECTION missing_url TO CONFLUENT SCHEMA REGISTRY (
    USERNAME 'foo'
  );
contains:must specify URL

! CREATE CONNECTION missing_cert TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://localhost',
    SSL KEY = SECRET s
  );
contains: requires both SSL KEY and SSL CERTIFICATE

! CREATE CONNECTION missing_key TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://localhost',
    SSL CERTIFICATE = ''
  );
contains: requires both SSL KEY and SSL CERTIFICATE

! CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}/FOO/BAR/BAZ'
  );
contains: URL must have an empty path

## SSH
! CREATE CONNECTION missing_user TO SSH TUNNEL (
    USER 'foo'
  );
contains: HOST option is required

## AWS PrivateLink

! CREATE CONNECTION conn1 TO KAFKA (BROKER '${testdrive.kafka-addr}' USING AWS PRIVATELINK foo (PORT 9093), SECURITY PROTOCOL PLAINTEXT);
contains: unknown catalog item 'foo'

! CREATE CONNECTION conn1 TO CONFLUENT SCHEMA REGISTRY (AWS PRIVATELINK foo, PORT 8080)
contains: unknown catalog item 'foo'

! CREATE CONNECTION pgconn TO POSTGRES (AWS PRIVATELINK foo, PORT 1234)
contains: unknown catalog item 'foo'

# Error in mzcompose: AWS PrivateLink connections are not supported
# Error in cloudtest/K8s: creating AWS PrivateLink Connection would violate max_aws_privatelink_connections limit
! CREATE CONNECTION privatelinkconn TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
  )
contains: AWS PrivateLink
