# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that the UUID type is properly decoded
#

$ set uuid={"type": "record", "name": "field_null", "fields": [ { "name": "f1", "type": { "logicalType": "uuid", "type": "string" } } ] }

$ kafka-create-topic topic=avro-types-uuid

$ kafka-ingest format=avro topic=avro-types-uuid schema=${uuid} timestamp=1
{"f1": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}

> CREATE SOURCE avro_types_uuid
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-types-uuid-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${uuid}'
  ENVELOPE NONE

> SELECT pg_typeof(f1), f1 FROM avro_types_uuid
uuid a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
