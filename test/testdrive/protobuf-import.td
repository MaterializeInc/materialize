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


# First, test without the schema registry.

$ file-append path=empty.proto
\${empty-schema}

$ file-append path=importee.proto
\${importee-schema}

$ file-append path=importer.proto
\${importer-schema}

$ protobuf-compile-descriptors inputs=empty.proto,importee.proto,importer.proto output=import.pb

$ kafka-create-topic topic=import partitions=1

$ kafka-ingest topic=import format=protobuf descriptor-file=import.pb message=Importer
{"importee1": {"b": false}, "importee2": {"ts": "1970-01-01T00:20:34.000005678Z"}}

> CREATE SOURCE import FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Importer' USING SCHEMA FILE '${testdrive.temp-dir}/import.pb'

> SELECT importee1::text, importee2::text FROM import
importee1  importee2
------------------------------
(f)        "(\"(1234,5678)\")"


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

$ schema-registry-publish subject=empty.proto schema-type=protobuf
\${empty-schema}

$ schema-registry-publish subject=importee.proto schema-type=protobuf references=google/protobuf/timestamp.proto
\${importee-schema}

$ schema-registry-publish subject=testdrive-import-csr-${testdrive.seed}-value schema-type=protobuf references=empty.proto,importee.proto
\${importer-schema}

$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=Importer confluent-wire-format=true
{"importee1": {"b": false}, "importee2": {"ts": "1970-01-01T00:20:34.000005678Z"}}

> CREATE SOURCE import_csr FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-import-csr-${testdrive.seed}'
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT importee1::text, importee2::text FROM import_csr
importee1  importee2
-------------------------------
(f)        "(\"(1234,5678)\")"

# Test that non-zero message IDs in the Confluent wire format are rejected.
$ kafka-ingest topic=import-csr format=protobuf descriptor-file=import.pb message=Importer confluent-wire-format=true schema-message-id=123
{"importee1": {"b": false}, "importee2": {"ts": "1970-01-01T00:20:34.000005678Z"}}

! SELECT importee1::text, importee2::text FROM import_csr
contains:Decode error: Text: protobuf deserialization error: unsupported Confluent-style protobuf message descriptor id: expected 0, but found: 123. See https://github.com/MaterializeInc/materialize/issues/9250
