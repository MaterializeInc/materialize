# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Set up a postgres source with one hydrated table. The upstream tables that
# are added to the source later already exist and have data. `t_alongside` is
# created upstream before `t_paused` so it has the smaller OID: the snapshot
# operator copies tables in OID order, so `t_alongside` finishes its copy
# before the `pg_snapshot_pause` failpoint holds `t_paused`.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;

CREATE TABLE t_before (f1 INTEGER);
ALTER TABLE t_before REPLICA IDENTITY FULL;
INSERT INTO t_before VALUES (1);

CREATE TABLE t_alongside (f1 INTEGER);
ALTER TABLE t_alongside REPLICA IDENTITY FULL;
INSERT INTO t_alongside VALUES (10), (20);

CREATE TABLE t_paused (f1 INTEGER);
ALTER TABLE t_paused REPLICA IDENTITY FULL;
INSERT INTO t_paused VALUES (100), (200);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg CASCADE;
> DROP CLUSTER IF EXISTS storage CASCADE;

> CREATE CLUSTER storage REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      STORAGE ADDRESSES ['clusterd1:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102'],
      WORKERS 4
    )
  )

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source');

> CREATE TABLE t_before FROM SOURCE mz_source (REFERENCE t_before);

> SELECT * FROM t_before
1
