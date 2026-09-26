# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# EXPLAIN ANALYZE INGESTION for a load generator source with several exports
# and a Kafka upsert table. Statistics and statuses arrive over time, so every
# value but the object is masked, and testdrive's retries cover the time they
# take to appear.

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_explain_analyze_storage_objects = true

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE auction
  FROM LOAD GENERATOR AUCTION (TICK INTERVAL '100ms')
  FOR ALL TABLES

> CREATE TABLE bids_tbl FROM SOURCE auction (REFERENCE "auction"."bids")

$ kafka-create-topic topic=ingestion-upsert partitions=1

$ kafka-ingest topic=ingestion-upsert format=bytes key-format=bytes key-terminator=:
one:horse
two:bee
one:cow

> CREATE SOURCE kafka_src
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-ingestion-upsert-${testdrive.seed}')

> CREATE TABLE upsert_tbl FROM SOURCE kafka_src (REFERENCE "testdrive-ingestion-upsert-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE UPSERT

> SELECT key, text FROM upsert_tbl
one cow
two bee

$ set-regex match=^(<null>|starting|running|-?\d+(\.\d+)?(\sbytes|\skB|\sMB|\sGB)?|\d{2}:\d{2}:\d{2}(\.\d+)?)$ replacement=<X>

# One row for the source and one per export other than the source's own.

> EXPLAIN ANALYZE INGESTION FOR SOURCE auction
public.auction        <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.accounts       <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.auctions       <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.bids           <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.bids_tbl       <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.organizations  <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.users          <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>

> EXPLAIN ANALYZE INGESTION FOR TABLE bids_tbl
public.bids_tbl       <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>

> EXPLAIN ANALYZE INGESTION FOR SOURCE kafka_src
public.kafka_src      <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>
public.upsert_tbl     <X> <X> <X> <X> <X> <X> <X> <X> <X> <X>

> DROP SOURCE kafka_src CASCADE

> DROP SOURCE auction CASCADE

> DROP CONNECTION kafka_conn

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_explain_analyze_storage_objects
