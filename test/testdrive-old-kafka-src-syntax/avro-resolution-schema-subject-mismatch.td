# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

#
# Test the case where a message with one schema is published to a topic with a different schema
#

$ set topic-name="resolution-schema-name-mismatch"

$ set schema1={"type": "record", "name": "schema1", "fields": [ {"name": "f1", "type": "int"} ] }
$ kafka-create-topic topic=${topic-name}
$ kafka-ingest format=avro topic=${topic-name} schema=${schema1} timestamp=1
{"f1": 123}

# this is simply to register a second schema with a different name
$ set schema2={"type": "record", "name": "schema2", "fields": [ {"name": "f1", "type": "int"} ] }
$ kafka-create-topic topic="${topic-name}2"
$ kafka-ingest format=avro topic="${topic-name}2" schema=${schema2} timestamp=1
{"f1": 123}

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
DROP SCHEMA IF EXISTS public CASCADE
CREATE SCHEMA public
GRANT CREATE, USAGE ON SCHEMA public TO materialize

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE resolution_schema_name_mismatch
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-${topic-name}-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

# The value is {"f1": 123}, with schema_id 2 but published to the topic related to schema_id 1
# We encode it manually to avoid publishing it.
$ kafka-ingest format=bytes topic=${topic-name} timestamp=1
\\x00\x00\x00\x00\x02\xf6\x01

! SELECT * FROM resolution_schema_name_mismatch
contains:to resolve writer schema node named
