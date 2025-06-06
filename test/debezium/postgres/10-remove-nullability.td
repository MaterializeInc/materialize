# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Change the definition of a column to be NOT NULL
#

# TODO: Reenable when database-issues#2047 is fixed
$ skip-if
SELECT true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_remove_nullability (f1 INTEGER);
ALTER TABLE alter_remove_nullability REPLICA IDENTITY FULL;
INSERT INTO alter_remove_nullability VALUES (123),(234);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE alter_remove_nullability
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_remove_nullability');

> CREATE TABLE alter_remove_nullability_tbl FROM SOURCE alter_remove_nullability (REFERENCE "postgres.public.alter_remove_nullability")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_remove_nullability_tbl;
123
234

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_remove_nullability ALTER COLUMN f1 SET NOT NULL;
INSERT INTO alter_remove_nullability VALUES (345);
UPDATE alter_remove_nullability SET f1 = 456 WHERE f1 = 123;
DELETE FROM alter_remove_nullability WHERE f1 = 234;

> SELECT * FROM alter_remove_nullability_tbl;
345
456
