# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Concurrent update on the same table.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE concurrent_update (f1 INTEGER, f2 TEXT);
ALTER TABLE concurrent_update REPLICA IDENTITY FULL;
INSERT INTO concurrent_update VALUES (1, 'r1');
INSERT INTO concurrent_update VALUES (11, 'r2');

$ schema-registry-wait-schema schema=postgres.public.concurrent_update-value

> CREATE SOURCE concurrent_update
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.concurrent_update'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
UPDATE concurrent_update SET f1 = f1 + 20 WHERE f1 = 1;

$ postgres-execute connection=conn2
BEGIN;
UPDATE concurrent_update SET f1 = f1 + 20 WHERE f1 = 11;

$ postgres-execute connection=conn1
UPDATE concurrent_update SET f1 = f1 * 2 WHERE f1 = 21;
COMMIT;

$ postgres-execute connection=conn2
UPDATE concurrent_update SET f1 = f1 * 10 WHERE f1 = 31;
COMMIT;

> SELECT * FROM concurrent_update;
42 r1
310 r2
