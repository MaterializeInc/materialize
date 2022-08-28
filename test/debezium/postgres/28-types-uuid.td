# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the UUID type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE uuid_type (pk_col UUID PRIMARY KEY, nopk_col UUID);
ALTER TABLE uuid_type REPLICA IDENTITY FULL;
INSERT INTO uuid_type VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');

$ schema-registry-wait-schema schema=postgres.public.uuid_type-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE uuid_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.uuid_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

#
# UUID columns arrive as string from Debezium without a logicalType
# Debezium ticket: DBZ-3555 UUID columns from Postgres are not registered as logicalType uuid in the Schema Registry
#
# The relevant part of the schema looks like this:
#
#                   {
#                     "type" : {
#                        "connect.version" : 1,
#                        "type" : "string",
#                        "connect.name" : "io.debezium.data.Uuid"
#                     },
#                     "name" : "pk_col"
#                  }
#

> SELECT pk_col, nopk_col, pg_typeof(pk_col) FROM uuid_type;
a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 text

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE uuid_type SET nopk_col = 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';

> SELECT pk_col, nopk_col FROM uuid_type;
a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
