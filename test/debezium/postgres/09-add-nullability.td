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
CREATE TABLE alter_allow_nullability (f1 INTEGER NOT NULL, f2 INTEGER PRIMARY KEY);
ALTER TABLE alter_allow_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_allow_nullability VALUES (123, 0),(234, 1);

$ schema-registry-wait topic=postgres.public.alter_allow_nullability

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE alter_allow_nullability
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_allow_nullability');

> CREATE TABLE alter_allow_nullability_tbl FROM SOURCE alter_allow_nullability (REFERENCE "postgres.public.alter_allow_nullability")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT f1 FROM alter_allow_nullability_tbl;
123
234

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_allow_nullability ALTER COLUMN f1 DROP NOT NULL;
INSERT INTO alter_allow_nullability VALUES (NULL, 2);
UPDATE alter_allow_nullability SET f1 = NULL WHERE f1 = 123;
DELETE FROM alter_allow_nullability WHERE f1 = 234;

! SELECT * FROM alter_allow_nullability_tbl;
contains:Decode error: Decoding error: Reader expected variant at index 1, got 0
