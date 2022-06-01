# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test that large transactions are properly replicated

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE IF EXISTS ten;
CREATE TABLE ten (f1 INTEGER);
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
CREATE TABLE large_distinct_rows (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE large_distinct_rows REPLICA IDENTITY FULL;
CREATE TABLE large_same_rows (f1 INTEGER);
ALTER TABLE large_same_rows REPLICA IDENTITY FULL;
CREATE SEQUENCE large_transaction_sequence;
BEGIN;
INSERT INTO large_distinct_rows SELECT nextval('large_transaction_sequence') FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4;
INSERT INTO large_same_rows SELECT 1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4;
COMMIT;

$ schema-registry-wait-schema schema=postgres.public.large_distinct_rows-value

$ schema-registry-wait-schema schema=postgres.public.large_same_rows-value

> CREATE MATERIALIZED SOURCE postgres_tx_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.transaction'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE;

> CREATE MATERIALIZED SOURCE large_distinct_rows
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.large_distinct_rows'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (SOURCE postgres_tx_metadata, COLLECTION 'public.large_distinct_rows')
  );

> CREATE MATERIALIZED SOURCE large_same_rows
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.large_same_rows'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM (
      TRANSACTION METADATA (SOURCE postgres_tx_metadata, COLLECTION 'public.large_same_rows')
  );


> SELECT COUNT(*), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM large_distinct_rows
10000 10000 1 10000

> SELECT COUNT(*), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM large_same_rows;
10000 1 1 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE large_distinct_rows SET f1 = f1 + 10000;
UPDATE large_same_rows SET f1 = 2;

> SELECT COUNT(*), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM large_distinct_rows
10000 10000 10001 20000

> SELECT COUNT(*), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM large_same_rows
10000 1 2 2

# Check that things are transactionally grouped as expected
> SELECT event_count, data_collections::text FROM postgres_tx_metadata WHERE event_count > 0 AND (data_collections::text LIKE '%large_distinct_rows%' OR data_collections::text LIKE '%large_same_rows%') ORDER BY id ASC
event_count data_collections
-----------------------------
20000       "{\"(public.large_distinct_rows,10000)\",\"(public.large_same_rows,10000)\"}"
20000       "{\"(public.large_distinct_rows,20000)\"}"
10000       "{\"(public.large_same_rows,10000)\"}"
