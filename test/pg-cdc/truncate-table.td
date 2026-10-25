# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test TRUNCATE -- source will error out
#

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
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

ANALYZE;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> COMMIT

> SELECT * FROM t1;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t1;

# Note: truncating the upstream table will not corrupt the source in Materialize
# for Postgres version 10. For all versions > 10, users will see the following error
# "source table got truncated."
#
# See the Postgres docs for more information:
# https://www.postgresql.org/docs/10/logical-replication-restrictions.html
! SELECT * FROM t1;
contains:table was truncated

#
# A truncate ends ingestion of the table, so its shard is sealed. A dataflow
# restart leaves a sealed table out. Resuming it from its last upper would miss
# the changes made upstream in between.
#

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}', REPLICATION FACTOR 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t_sealed (f1 INTEGER);
ALTER TABLE t_sealed REPLICA IDENTITY FULL;
INSERT INTO t_sealed VALUES (1);

CREATE TABLE t_live (f1 INTEGER);
ALTER TABLE t_live REPLICA IDENTITY FULL;
INSERT INTO t_live VALUES (1);

CREATE TABLE t_rewound (f1 INTEGER);
ALTER TABLE t_rewound REPLICA IDENTITY FULL;
INSERT INTO t_rewound VALUES (1);

CREATE PUBLICATION mz_source_seal FOR TABLE t_sealed, t_live, t_rewound;

> BEGIN
> CREATE SOURCE mz_source_seal
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_seal');

> CREATE TABLE t_sealed FROM SOURCE mz_source_seal (REFERENCE t_sealed);
> CREATE TABLE t_live FROM SOURCE mz_source_seal (REFERENCE t_live);
> COMMIT

> SELECT * FROM t_sealed;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t_sealed;
INSERT INTO t_sealed VALUES (2);

! SELECT * FROM t_sealed;
contains:table was truncated

> SELECT write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON id = object_id
  WHERE name = 't_sealed'
true

# Removing the replicas stops the LSN feedback, so everything written upstream
# from here on is replayed once a replica comes back.
> ALTER CLUSTER storage SET (REPLICATION FACTOR 0)

$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM t_sealed;
INSERT INTO t_live VALUES (2);

# This table is snapshotted after the truncate below, so the rewind of its
# snapshot reaches back past it. The snapshot no longer holds the rows the
# truncate removed, so the table is in error from the start.
> CREATE TABLE t_rewound FROM SOURCE mz_source_seal (REFERENCE t_rewound);

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t_rewound;
INSERT INTO t_rewound VALUES (2);

> ALTER CLUSTER storage SET (REPLICATION FACTOR 1)

> SELECT * FROM t_live;
1
2

! SELECT * FROM t_sealed;
contains:table was truncated

! SELECT * FROM t_rewound;
contains:table was truncated

> SELECT name, write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON id = object_id
  WHERE name IN ('t_sealed', 't_live', 't_rewound')
t_live false
t_rewound true
t_sealed true

> DROP SOURCE mz_source_seal CASCADE
