# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# INSERT + DELETE in same transaction should not be visible to Mz
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE insert_delete (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE insert_delete REPLICA IDENTITY FULL;
INSERT INTO insert_delete VALUES (1);

$ schema-registry-wait topic=postgres.public.insert_delete

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE insert_delete
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.insert_delete');

> CREATE TABLE insert_delete_tbl FROM SOURCE insert_delete (REFERENCE "postgres.public.insert_delete")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
INSERT INTO insert_delete VALUES (2);
DELETE FROM insert_delete WHERE f1 = 2;
COMMIT;

> SELECT * FROM insert_delete_tbl;
1
