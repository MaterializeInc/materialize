# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# An incompatible schema change ends ingestion of the table, so its shard is
# sealed. A dataflow restart leaves a sealed table out. Resuming it from its
# last upper would miss the changes made upstream in between.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET pg_schema_validation_interval = '2s';

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}', REPLICATION FACTOR 1

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
DROP PUBLICATION IF EXISTS mz_source;

CREATE TABLE t_relation (f1 INTEGER);
ALTER TABLE t_relation REPLICA IDENTITY FULL;
INSERT INTO t_relation VALUES (1);

CREATE TABLE t_validated (f1 INTEGER);
ALTER TABLE t_validated REPLICA IDENTITY FULL;
INSERT INTO t_validated VALUES (1);

CREATE TABLE t_snapshot (f1 INTEGER);
ALTER TABLE t_snapshot REPLICA IDENTITY FULL;
INSERT INTO t_snapshot VALUES (1);

CREATE TABLE t_live (f1 INTEGER);
ALTER TABLE t_live REPLICA IDENTITY FULL;
INSERT INTO t_live VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t_relation FROM SOURCE mz_source (REFERENCE t_relation);
> CREATE TABLE t_validated FROM SOURCE mz_source (REFERENCE t_validated);
> CREATE TABLE t_live FROM SOURCE mz_source (REFERENCE t_live);
> COMMIT

> SELECT * FROM t_relation;
1

> SELECT * FROM t_validated;
1

# The insert sends a Relation message that reveals the change. Without one, the
# periodic schema validation finds it.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t_relation ALTER COLUMN f1 TYPE bigint;
INSERT INTO t_relation VALUES (2);
ALTER TABLE t_validated ALTER COLUMN f1 TYPE bigint;

! SELECT * FROM t_relation;
contains:incompatible schema change

# The validation seals the table at the end of the upstream WAL. Its error gets
# a timestamp once the upstream writes past that LSN.
> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 't_validated'
stalled

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_live VALUES (2);

! SELECT * FROM t_validated;
contains:incompatible schema change

> SELECT name, write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON id = object_id
  WHERE name IN ('t_relation', 't_validated')
t_relation true
t_validated true

# Removing the replicas stops the LSN feedback, so everything written upstream
# from here on is replayed once a replica comes back.
> ALTER CLUSTER storage SET (REPLICATION FACTOR 0)

# This table captures the schema before the change below, so its snapshot
# finds the change and copies nothing.
> CREATE TABLE t_snapshot FROM SOURCE mz_source (REFERENCE t_snapshot);

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t_snapshot ALTER COLUMN f1 TYPE bigint;
DELETE FROM t_relation;
DELETE FROM t_validated;
INSERT INTO t_live VALUES (3);

> ALTER CLUSTER storage SET (REPLICATION FACTOR 1)

> SELECT * FROM t_live;
1
2
3

! SELECT * FROM t_relation;
contains:incompatible schema change

! SELECT * FROM t_validated;
contains:incompatible schema change

! SELECT * FROM t_snapshot;
contains:incompatible schema change

> SELECT name, write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON id = object_id
  WHERE name IN ('t_relation', 't_validated', 't_snapshot', 't_live')
t_live false
t_relation true
t_snapshot true
t_validated true

> DROP SOURCE mz_source CASCADE
> DROP CLUSTER storage
