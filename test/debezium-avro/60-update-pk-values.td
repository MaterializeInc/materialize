# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that moving a record from one PK to another works
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE IF EXISTS ten;
CREATE TABLE update_pk_values (f1 INTEGER, f2 INTEGER, PRIMARY KEY (f1));
ALTER TABLE update_pk_values REPLICA IDENTITY FULL;
BEGIN;
INSERT INTO update_pk_values VALUES (1, 1);
COMMIT;

$ schema-registry-wait-schema schema=postgres.public.update_pk_values-value

> CREATE MATERIALIZED SOURCE update_pk_values
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.update_pk_values'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT COUNT(*) > 0 FROM update_pk_values;
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO update_pk_values VALUES (11, 11);
UPDATE update_pk_values SET f1 = f1 + 1;

> SELECT * FROM update_pk_values;
2 1
12 11
