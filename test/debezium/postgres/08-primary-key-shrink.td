# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Shrink the number of columns that participate in a primary key
# Currently, this throws an error on the Debezium side and replication stops
#

# TODO: Reenable when database-issues#2047 is fixed
$ skip-if
SELECT true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_shrink_primary_key (f1 INTEGER DEFAULT 1, f2 INTEGER DEFAULT 2, PRIMARY KEY (f1, f2));
ALTER TABLE alter_shrink_primary_key REPLICA IDENTITY FULL;
INSERT INTO alter_shrink_primary_key VALUES (123,234),(345,456);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE alter_shrink_primary_key
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_shrink_primary_key');

> CREATE TABLE alter_shrink_primary_key_tbl FROM SOURCE alter_shrink_primary_key (REFERENCE "postgres.public.alter_shrink_primary_key")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

> SELECT * FROM alter_shrink_primary_key_tbl;
123 234
345 456

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_shrink_primary_key DROP COLUMN f2, ADD PRIMARY KEY (f1);
INSERT INTO alter_shrink_primary_key VALUES (567);
DELETE FROM alter_shrink_primary_key WHERE f1 = 123;

> SELECT * FROM alter_shrink_primary_key_tbl;
345
567
