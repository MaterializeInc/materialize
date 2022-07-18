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
CREATE TABLE update_pk_values (f1 INTEGER, f2 INTEGER, PRIMARY KEY (f1));
ALTER TABLE update_pk_values REPLICA IDENTITY FULL;
INSERT INTO update_pk_values VALUES (1, 10);
INSERT INTO update_pk_values VALUES (2, 20);

$ schema-registry-wait-schema schema=postgres.public.update_pk_values-value

> CREATE SOURCE update_pk_values
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.update_pk_values'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
UPDATE update_pk_values SET f1 = f1 + 10 , f2 = f2 + 10 WHERE f1 = 1;

$ postgres-execute connection=conn2
BEGIN;
UPDATE update_pk_values SET f1 = f1 + 10 , f2 = f2 + 10 WHERE f1 = 2;

$ postgres-execute connection=conn1
INSERT INTO update_pk_values VALUES (4, 40);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO update_pk_values VALUES (5, 50);
COMMIT;

> SELECT * FROM update_pk_values;
4  40
5  50
11 20
12 30
