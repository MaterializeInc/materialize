# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test DROP COLUMN on nullable columns
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_drop_column (f1 INTEGER, col_null_no_default INTEGER, col_null_default INTEGER DEFAULT 999);
INSERT INTO alter_drop_column VALUES (123, 234, 345);

$ schema-registry-wait-schema schema=postgres.public.alter_drop_column-value

> CREATE SOURCE alter_drop_column
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_drop_column'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_drop_column;
123 234 345

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_column DROP COLUMN col_null_default;
INSERT INTO alter_drop_column VALUES (123);

# Prior to Debezium 1.6, NULLs would be returned for col_null_default in new rows.
# Starting with Debezium 1.6, the 999 DEFAULT value will be returned instead.

> SELECT * FROM alter_drop_column;
123 234 345
123 <null> 999
