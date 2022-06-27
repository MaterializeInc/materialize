# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Remove NOT NULL constraint from column
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_allow_nullability (f1 INTEGER NOT NULL);
ALTER TABLE alter_allow_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_allow_nullability VALUES (123),(234);

$ schema-registry-wait-schema schema=postgres.public.alter_allow_nullability-value

> CREATE MATERIALIZED SOURCE alter_allow_nullability
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_allow_nullability'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_allow_nullability;
123
234

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_allow_nullability ALTER COLUMN f1 DROP NOT NULL;
INSERT INTO alter_allow_nullability VALUES (NULL);
UPDATE alter_allow_nullability SET f1 = NULL WHERE f1 = 123;
DELETE FROM alter_allow_nullability WHERE f1 = 234;

! SELECT * FROM alter_allow_nullability;
contains:Decode error: Decoding error: Reader expected variant at index 1, got 0
