# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that DROP COLUMN on a NOT NULL column is handled. No NULLs should
# appear in the stream at a column that was NOT NULLable
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_drop_column_not_null (f1 INTEGER, col_not_null INTEGER NOT NULL);
INSERT INTO alter_drop_column_not_null VALUES (123, 234);

$ schema-registry-wait-schema schema=postgres.public.alter_drop_column_not_null-value

> CREATE SOURCE alter_drop_column_not_null
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_drop_column_not_null'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_drop_column_not_null;
123 234

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_drop_column_not_null DROP COLUMN col_not_null;
INSERT INTO alter_drop_column_not_null VALUES (345);

# An error is thrown instead of returning nulls in a NOT NULL column

! SELECT * FROM alter_drop_column_not_null;
contains:Reader field `postgres.public.alter_drop_column_not_null.Value.col_not_null` not found in writer, and has no default
