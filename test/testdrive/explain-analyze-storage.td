# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# EXPLAIN ANALYZE for storage objects on a unified cluster: a load generator
# source with several exports, a Kafka upsert source, and a Kafka sink. Stage
# names and the tree shape are deterministic. Measurements, statistics,
# statuses, and ids are masked, and testdrive's retries cover the time the
# introspection relations take to catch up.
#
# The topology is baked in at replica provisioning, so the parameter must be
# set before the cluster is created.

$ set-arg-default default-replica-size=scale=1,workers=1

$ set-regex match=^\d{2}:\d{2}:\d{2}(\.\d+)?$|\bu\d+\b replacement=<X>

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unified_cluster = true
ALTER SYSTEM SET enable_explain_analyze_storage_objects = true
ALTER SYSTEM SET enable_load_generator_key_value = true

> CREATE CLUSTER uc (SIZE '${arg.default-replica-size}')

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE auction
  IN CLUSTER uc
  FROM LOAD GENERATOR AUCTION (TICK INTERVAL '100ms')
  FOR ALL TABLES

> CREATE TABLE bids_tbl FROM SOURCE auction (REFERENCE "auction"."bids")

$ kafka-create-topic topic=upsert partitions=1

$ kafka-ingest topic=upsert format=bytes key-format=bytes key-terminator=:
one:horse
two:bee
one:cow

> CREATE SOURCE kafka_src
  IN CLUSTER uc
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-${testdrive.seed}')

> CREATE TABLE upsert_tbl FROM SOURCE kafka_src (REFERENCE "testdrive-upsert-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE UPSERT

> SELECT key, text FROM upsert_tbl
one cow
two bee

> CREATE SOURCE kv
  IN CLUSTER uc
  FROM LOAD GENERATOR KEY VALUE (KEYS 16, PARTITIONS 4, SNAPSHOT ROUNDS 3, SEED 123, VALUE SIZE 10, BATCH SIZE 2, TICK INTERVAL '100ms')

> CREATE TABLE kv_upsert FROM SOURCE kv INCLUDE KEY ENVELOPE UPSERT

> CREATE SINK snk
  IN CLUSTER uc
  FROM upsert_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-explain-analyze-storage-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data format=json sink=materialize.public.snk key=false sort-messages=true
{"key": "one", "text": "cow"}
{"key": "two", "text": "bee"}

# Errors that need a sink to exist.

! EXPLAIN ANALYZE INGESTION FOR SINK snk
contains:EXPLAIN ANALYZE INGESTION is only supported for sources and tables created from sources

! EXPLAIN ANALYZE CPU FOR SINK snk
contains:runs on cluster "uc", but the active cluster is

> SET cluster = uc

# The rows are compared as a multiset, so the order of the tree is pinned by
# the ORDER BY in the AS SQL goldens (explain_analyze_storage.slt). The mask
# matches only a non-NULL interval, so every row, the root included, must have
# a total.

> EXPLAIN ANALYZE CPU FOR SOURCE auction
"Source auction (<X>)"                  <X>
"  Remap"                               <X>
"  Reader"                              <X>
"  Partition"                           <X>
"  Healthcheck"                         <X>
"  Export public.auction (<X>)"         <X>
"  Export public.accounts (<X>)"        <X>
"  Export public.auctions (<X>)"        <X>
"  Export public.bids (<X>)"            <X>
"  Export public.organizations (<X>)"   <X>
"  Export public.users (<X>)"           <X>
"  Export public.bids_tbl (<X>)"        <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Reclock"                           <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    Envelope NONE"                     <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>
"    PersistSink"                       <X>

# A Kafka export with an encoding has a Decode stage.

> EXPLAIN ANALYZE CPU FOR TABLE upsert_tbl
"Remap (shared)"                        <X>
"Reader (shared)"                       <X>
"Partition (shared)"                    <X>
"Healthcheck (shared)"                  <X>
"Export public.upsert_tbl (<X>)"        <X>
"  Reclock"                             <X>
"  Decode"                              <X>
"  Envelope UPSERT"                     <X>
"  PersistSink"                         <X>

> EXPLAIN ANALYZE CPU FOR SINK snk
"Sink snk (<X>)"                        <X>
"  Export public.snk (<X>)"             <X>
"    Arrange"                           <X>
"    Encode"                            <X>
"    Sink"                              <X>

# One row for the source and one per export other than the source's own.
# Statistics and statuses arrive over time, so every value but the object is
# masked.

# A load generator upsert export has an Envelope UPSERT stage and no Decode.

> EXPLAIN ANALYZE CPU FOR SOURCE kv
"Source kv (<X>)"                       <X>
"  Remap"                               <X>
"  Reader"                              <X>
"  Partition"                           <X>
"  Healthcheck"                         <X>
"  Export public.kv_upsert (<X>)"       <X>
"    Reclock"                           <X>
"    Envelope UPSERT"                   <X>
"    PersistSink"                       <X>

> EXPLAIN ANALYZE CPU FOR TABLE kv_upsert
"Remap (shared)"                        <X>
"Reader (shared)"                       <X>
"Partition (shared)"                    <X>
"Healthcheck (shared)"                  <X>
"Export public.kv_upsert (<X>)"         <X>
"  Reclock"                             <X>
"  Envelope UPSERT"                     <X>
"  PersistSink"                         <X>

# Measurements depend on timing, so the remaining checks mask every value and
# compare the column shapes and labels.

$ set-regex match=^(<null>|-?\d+(\.\d+)?(\sbytes|\skB|\sMB|\sGB)?|\d{2}:\d{2}:\d{2}(\.\d+)?)$|\bu\d+\b replacement=<X>

> EXPLAIN ANALYZE MEMORY, CPU FOR TABLE bids_tbl
"Remap (shared)"                        <X> <X> <X>
"Reader (shared)"                       <X> <X> <X>
"Partition (shared)"                    <X> <X> <X>
"Healthcheck (shared)"                  <X> <X> <X>
"Export public.bids_tbl (<X>)"          <X> <X> <X>
"  Reclock"                             <X> <X> <X>
"  Envelope NONE"                       <X> <X> <X>
"  PersistSink"                         <X> <X> <X>

> EXPLAIN ANALYZE CPU, MEMORY FOR SINK snk
"Sink snk (<X>)"                        <X> <X> <X>
"  Export public.snk (<X>)"             <X> <X> <X>
"    Arrange"                           <X> <X> <X>
"    Encode"                            <X> <X> <X>
"    Sink"                              <X> <X> <X>

# Storage stages are attributed to their objects alongside compute objects.

> EXPLAIN ANALYZE CLUSTER CPU
materialize.public.accounts       <X> <X>
materialize.public.auction        <X> <X>
materialize.public.auctions       <X> <X>
materialize.public.bids           <X> <X>
materialize.public.bids_tbl       <X> <X>
materialize.public.kafka_src      <X> <X>
materialize.public.kv             <X> <X>
materialize.public.kv_upsert      <X> <X>
materialize.public.organizations  <X> <X>
materialize.public.snk            <X> <X>
materialize.public.upsert_tbl     <X> <X>
materialize.public.users          <X> <X>

> RESET cluster

> DROP SINK snk

> DROP SOURCE kv CASCADE

> DROP SOURCE kafka_src CASCADE

> DROP SOURCE auction CASCADE

> DROP CLUSTER uc

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_unified_cluster
ALTER SYSTEM RESET enable_explain_analyze_storage_objects
ALTER SYSTEM RESET enable_load_generator_key_value
