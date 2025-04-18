# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test adding a primary key over an existing column
#

# TODO: Reenable when database-issues#2047 is fixed
$ skip-if
SELECT true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_add_primary_key (f1 INTEGER);
INSERT INTO alter_add_primary_key VALUES (123), (234);

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE alter_add_primary_key
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_add_primary_key');

> CREATE TABLE alter_add_primary_key_tbl FROM SOURCE alter_add_primary_key (REFERENCE "postgres.public.alter_add_primary_key")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_add_primary_key_tbl;
123
234

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_primary_key ADD PRIMARY KEY (f1);
INSERT INTO alter_add_primary_key VALUES (345);
DELETE FROM alter_add_primary_key WHERE f1 = 123;

> SELECT * FROM alter_add_primary_key_tbl;
234
345
