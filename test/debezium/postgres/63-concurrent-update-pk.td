# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Concurrent modifications of a PK
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE concurrent_update_pk (f1 INTEGER, f2 INTEGER, PRIMARY KEY (f1));
ALTER TABLE concurrent_update_pk REPLICA IDENTITY FULL;
INSERT INTO concurrent_update_pk VALUES (1, 10);
INSERT INTO concurrent_update_pk VALUES (2, 20);

$ schema-registry-wait-schema schema=postgres.public.concurrent_update_pk-value

> CREATE SOURCE concurrent_update_pk
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.concurrent_update_pk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
UPDATE concurrent_update_pk SET f1 = f1 + 10 , f2 = f2 + 10 WHERE f1 = 1;

$ postgres-execute connection=conn2
BEGIN;
UPDATE concurrent_update_pk SET f1 = f1 + 10 , f2 = f2 + 10 WHERE f1 = 2;

$ postgres-execute connection=conn1
INSERT INTO concurrent_update_pk VALUES (4, 40);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO concurrent_update_pk VALUES (5, 50);
COMMIT;

> SELECT * FROM concurrent_update_pk;
4 40
5 50
11 20
12 30
