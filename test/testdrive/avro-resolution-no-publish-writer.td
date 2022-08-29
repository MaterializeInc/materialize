# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the case where we fail to publish the schema of the writer
#

$ set int-schema={"type": "record", "name": "schema_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=resolution-no-publish-writer

$ kafka-ingest format=avro topic=resolution-no-publish-writer schema=${int-schema} timestamp=1
{"f1": 123}

> DROP SCHEMA IF EXISTS public CASCADE
> CREATE SCHEMA public

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE resolution_no_publish_writer
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-resolution-no-publish-writer-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

# The value is {"f1": 123}.
# We encode it manually to avoid publishing it.
$ kafka-ingest format=bytes topic=resolution-no-publish-writer timestamp=1
\\x00\x00\x00\x00\x01\xf6\x01

! SELECT * FROM resolution_no_publish_writer;
contains:to resolve
