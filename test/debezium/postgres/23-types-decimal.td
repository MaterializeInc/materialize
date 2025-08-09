# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the decimal type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE decimal_type (f1 DECIMAL(5,3), f2 INTEGER PRIMARY KEY);
ALTER TABLE decimal_type REPLICA IDENTITY FULL;
INSERT INTO decimal_type VALUES (NULL, 0), (NULL, 1), (12.345, 2), ('NaN', 3);

$ schema-registry-wait topic=postgres.public.decimal_type

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE decimal_type
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.decimal_type');

> CREATE TABLE decimal_type_tbl FROM SOURCE decimal_type (REFERENCE "postgres.public.decimal_type")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

# NaN arrives as NULL

> SELECT f1 FROM decimal_type_tbl;
12.345
<null>
<null>
<null>

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE decimal_type SET f1 = NULL WHERE f1 = 'NaN';
UPDATE decimal_type SET f1 = 0.123 WHERE f1 = 12.345;
UPDATE decimal_type SET f1 = 'NaN' WHERE f1 IS NULL;

> SELECT f1 FROM decimal_type_tbl;
<null>
<null>
<null>
0.123
