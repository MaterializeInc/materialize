# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that dropping the primary key is handled correctly
#

# TODO: Reenable when database-issues#2025 is fixed
$ skip-if
SELECT true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_drop_primary_key (f1 INTEGER PRIMARY KEY);
INSERT INTO alter_drop_primary_key VALUES (123);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE alter_drop_primary_key
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_drop_primary_key');

> CREATE TABLE alter_drop_primary_key_tbl FROM SOURCE alter_drop_primary_key (REFERENCE "postgres.public.alter_drop_primary_key")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_drop_primary_key_tbl;
123

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE SEQUENCE pk_sequence;
ALTER TABLE alter_drop_primary_key DROP CONSTRAINT alter_drop_primary_key_pkey;
INSERT INTO alter_drop_primary_key VALUES (123);

> SELECT COUNT(*) FROM alter_drop_primary_key_tbl;
2

> SELECT DISTINCT f1 FROM alter_drop_primary_key_tbl;
123
