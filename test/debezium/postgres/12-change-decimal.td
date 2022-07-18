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
# see discussion in https://github.com/MaterializeInc/materialize/issues/6536
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_change_decimal (f1 DECIMAL(5,3));
ALTER TABLE alter_change_decimal REPLICA IDENTITY FULL;
INSERT INTO alter_change_decimal VALUES (0),(NULL),(12.345);

$ schema-registry-wait-schema schema=postgres.public.alter_change_decimal-value

> CREATE SOURCE alter_change_decimal
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_change_decimal'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_change_decimal;
<null>
0
12.345

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_change_decimal ALTER COLUMN f1 TYPE DECIMAL(6,4);
INSERT INTO alter_change_decimal VALUES (23.456);
UPDATE alter_change_decimal SET f1 = 34.567 WHERE f1 = 0;

! SELECT * FROM alter_change_decimal;
contains:Decimal types must match in precision, scale, and fixed size
