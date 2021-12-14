# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Protobuf imports are handled correctly.

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

# Setup a similar, other schema for schema evolution checks
$ set importer-other-schema
syntax = "proto3";

import "empty.proto";
import "importee.proto";

message Importer {
    Importee1 importee3 = 1;
    Importee2 importee4 = 2;
}


# First, test without the schema registry.

$ file-append path=empty.proto
\${empty-schema}

$ file-append path=importee.proto
\${importee-schema}

$ file-append path=importer.proto
\${importer-schema}

$ protobuf-compile-descriptors inputs=empty.proto,importee.proto,importer.proto output=import.pb

$ kafka-create-topic topic=import partitions=1

$ kafka-ingest topic=import format=protobuf descriptor-file=import.pb message=.Importer
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

> CREATE MATERIALIZED SOURCE import FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Importer' USING SCHEMA FILE '${testdrive.temp-dir}/import.pb'

> SELECT importee1::text, importee2::text, mz_offset FROM import
importee1  importee2            mz_offset
-----------------------------------------
(f)        "(\"(1234,5678)\")"  1


# Then, test again with the Confluent Schema Registry. Publishing Protobuf
# schemas to the CSR is tricky, because each Protobuf file needs to go into its
# own subject. This is handled automatically by the Java Kafka client, but it is
# complicated to duplicate that automatic logic in Testdrive. So we publish
# the schemas manually here, but keep the schema very simple to reduce the pain.

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

# publish all the schemas
$ schema-registry-publish subject=empty.proto schema-type=protobuf
\${empty-schema}

$ schema-registry-publish subject=importee.proto schema-type=protobuf references=google/protobuf/timestamp.proto
\${importee-schema}

$ schema-registry-publish subject=testdrive-import-csr-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

$ schema-registry-publish subject=testdrive-import-csr-other-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

$ schema-registry-publish subject=testdrive-import-csr-other2-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-other-schema}

$ schema-registry-publish subject=testdrive-import-csr-other3-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

# schema id 4 is the correct schema
$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=4
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

> CREATE MATERIALIZED SOURCE import_csr FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT importee1::text, importee2::text, mz_offset FROM import_csr
importee1  importee2            mz_offset
-----------------------------------------
(f)        "(\"(1234,5678)\")"  1

# test input
$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=4
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

> SELECT importee1::text, importee2::text, mz_offset FROM import_csr
importee1  importee2            mz_offset
-----------------------------------------
(f)        "(\"(1234,5678)\")"  1

# Also test that our schema id checks make sense
$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=4 schema-message-id=123
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

! SELECT importee1::text, importee2::text, mz_offset FROM import_csr
Decode error: Text: protobuf deserialization error: unsupported Confluent-style protobuf message descriptor id: expected 0, but found: 123. See https://github.com/MaterializeInc/materialize/issues/9250


# Schema id checks
# Setup schemas for schema recompiliation tests
$ file-append path=importer-other.proto
\${importer-other-schema}

$ schema-registry-publish subject=testdrive-import-csr-wrong-id-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

$ schema-registry-publish subject=testdrive-import-csr-wrong-proto-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

# the wrong schema that will have id 12
$ schema-registry-publish subject=testdrive-import-csr-other-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-other-schema}


# wrong id case
# Setup the correct first message, priming all the workers with the correct schema id
$ kafka-ingest repeat=9 topic=import-csr-wrong-id format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=4
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

# Setup the source
> CREATE MATERIALIZED SOURCE import_csr_wrong_id FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-wrong-id-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT importee1::text, importee2::text, mz_offset FROM import_csr_wrong_id
importee1  importee2            mz_offset
-----------------------------------------
(f)        "(\"(1234,5678)\")"  1
(f)        "(\"(1234,5678)\")"  2
(f)        "(\"(1234,5678)\")"  3
(f)        "(\"(1234,5678)\")"  4
(f)        "(\"(1234,5678)\")"  5
(f)        "(\"(1234,5678)\")"  6
(f)        "(\"(1234,5678)\")"  7
(f)        "(\"(1234,5678)\")"  8
(f)        "(\"(1234,5678)\")"  9

# Send a message with the wrong schema id
$ kafka-ingest topic=import-csr-wrong-id format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=12
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

! SELECT importee1::text, importee2::text, mz_offset FROM import_csr_wrong_id
Decode error: Text: protobuf deserialization error: cannot decode protobuf, expected schema id: 4, found 12; schema evolution in protobuf is not supported


# wrong protobuf definition case
# Setup the source
> CREATE MATERIALIZED SOURCE import_csr_wrong_proto FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-wrong-proto-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Send a message with the wrong schema id
$ kafka-ingest topic=import-csr-wrong-proto format=protobuf descriptor-file=import.pb message=.Importer confluent-wire-format=true schema-id=12
{"importee1": {"b": false}, "importee2": {"ts": {"seconds": 1234, "nanos": 5678}}}

! SELECT importee1::text, importee2::text, mz_offset FROM import_csr_wrong_proto
Decode error: Text: protobuf deserialization error: cannot decode protobuf, schema id: 12 refers to a schema different than the expected schema; schema evolution in protobuf is not supported.
