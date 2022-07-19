# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that adding a column does not mess things up.
# We expect that updates where the data is only different in this column will
# be handled correctly.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_add_column (f1 INTEGER);
ALTER TABLE alter_add_column REPLICA IDENTITY FULL;
INSERT INTO alter_add_column VALUES (123);

$ schema-registry-wait-schema schema=postgres.public.alter_add_column-value

> CREATE SOURCE alter_add_column
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_add_column'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_add_column;
123

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_add_column ADD COLUMN new_column INTEGER DEFAULT 1;
INSERT INTO alter_add_column VALUES (123,2);

UPDATE alter_add_column SET f1 = f1 * 10 WHERE new_column = 1;
UPDATE alter_add_column SET f1 = f1 * 100 WHERE new_column = 2;

# Even though we do not have new_column in our source, we expect that the
# updates above have landed on the appropriate distinct rows

> SELECT * FROM alter_add_column;
1230
12300
