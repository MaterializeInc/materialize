# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Changing the definition of a DECIMAL column results in a source error
# see discussion in https://github.com/MaterializeInc/database-issues/issues/2032
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_change_decimal (f1 DECIMAL(5,3), f2 INTEGER PRIMARY KEY);
ALTER TABLE alter_change_decimal REPLICA IDENTITY FULL;
INSERT INTO alter_change_decimal VALUES (0, 0),(NULL, 1),(12.345, 2);

$ schema-registry-wait topic=postgres.public.alter_change_decimal

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE alter_change_decimal
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.alter_change_decimal');

> CREATE TABLE alter_change_decimal_tbl FROM SOURCE alter_change_decimal (REFERENCE "postgres.public.alter_change_decimal")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

> SELECT f1 FROM alter_change_decimal_tbl;
<null>
0
12.345

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_change_decimal ALTER COLUMN f1 TYPE DECIMAL(6,4);
INSERT INTO alter_change_decimal VALUES (23.456, 3);
UPDATE alter_change_decimal SET f1 = 34.567 WHERE f1 = 0;

! SELECT * FROM alter_change_decimal_tbl;
contains:Decimal types must match in precision, scale, and fixed size
