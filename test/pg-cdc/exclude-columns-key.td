# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# We can support the exclusion of constraint columns, and any constraint for which one or more
# columns is excluded will simply be ignored.
#

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}'

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
CREATE PUBLICATION mz_source FOR ALL TABLES;

# A table with a PRIMARY KEY: `id` (attnum 1) participates in the key.
CREATE TABLE pk_table (id int PRIMARY KEY, val text);
INSERT INTO pk_table VALUES (1, 'a'), (2, 'b');
ALTER TABLE pk_table REPLICA IDENTITY FULL;

# A table with a UNIQUE constraint: `(id, u)`
CREATE TABLE uniq_table (id int NOT NULL, u int NOT NULL, val text NOT NULL, UNIQUE (id, u));
INSERT INTO uniq_table VALUES (1, 10, 'a'), (2, 20, 'b');
ALTER TABLE uniq_table REPLICA IDENTITY FULL;

# A table with a composite PRIMARY KEY: `(id, sub_id)`. Excluding just one of the
# two columns must drop the whole key, not leave a partial one behind.
CREATE TABLE composite_pk_table (id int NOT NULL, sub_id int NOT NULL, val text NOT NULL, PRIMARY KEY (id, sub_id));
INSERT INTO composite_pk_table VALUES (1, 10, 'a'), (2, 20, 'b');
ALTER TABLE composite_pk_table REPLICA IDENTITY FULL;

# A table with two independent keys: `PRIMARY KEY (id)` and `UNIQUE (u)`. Excluding
# `id` must drop only the key that names it, leaving `UNIQUE (u)` intact.
CREATE TABLE multi_key_table (id int NOT NULL, u int NOT NULL, val text NOT NULL, PRIMARY KEY (id), UNIQUE (u));
INSERT INTO multi_key_table VALUES (1, 10, 'a'), (2, 20, 'b');
ALTER TABLE multi_key_table REPLICA IDENTITY FULL;

ANALYZE;

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);

# Excluding the PRIMARY KEY column should be fine
> CREATE TABLE pk_table
  FROM SOURCE mz_source (REFERENCE public.pk_table)
  WITH (EXCLUDE COLUMNS (id));

# Excluding a UNIQUE-constraint column also okay
> CREATE TABLE uniq_table
  FROM SOURCE mz_source (REFERENCE public.uniq_table)
  WITH (EXCLUDE COLUMNS (u));

# Excluding one column of a composite PRIMARY KEY drops the whole key
> CREATE TABLE composite_pk_table
  FROM SOURCE mz_source (REFERENCE public.composite_pk_table)
  WITH (EXCLUDE COLUMNS (id));

# Excluding the PRIMARY KEY's column drops that key, but the unrelated
# UNIQUE (u) constraint is unaffected
> CREATE TABLE multi_key_table
  FROM SOURCE mz_source (REFERENCE public.multi_key_table)
  WITH (EXCLUDE COLUMNS (id));

# Verify that the constraint is ignored on uniq_table
> CREATE DEFAULT INDEX ON uniq_table;

> SELECT key FROM (SHOW INDEXES ON uniq_table);
{id,val}

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT id FROM uniq_table
Explained Query:
  Distinct project=[#0]
    Project (#0)
      ReadIndex on=uniq_table uniq_table_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.uniq_table_primary_idx (*** full scan ***)

Target cluster: quickstart

# Verify that excluding one column of a composite key drops the whole key: no
# unique key remains, so the default index is a full scan over every column.
> CREATE DEFAULT INDEX ON composite_pk_table;

> SELECT key FROM (SHOW INDEXES ON composite_pk_table);
{sub_id,val}

# Verify that excluding the PRIMARY KEY's column only drops that key; the
# unrelated UNIQUE (u) constraint still makes `u` a valid index key.
> CREATE DEFAULT INDEX ON multi_key_table;

> SELECT key FROM (SHOW INDEXES ON multi_key_table);
{u}

# Sanity: excluding a non-key column on a keyed table still works, and the key
# constraint is preserved on the resulting table.
> CREATE TABLE pk_table_2
  FROM SOURCE mz_source (REFERENCE public.pk_table)
  WITH (EXCLUDE COLUMNS (val));

> SELECT * FROM pk_table_2;
1
2

#
# Regression: excluding a key column then dropping it upstream must not halt the
# source. Purification prunes the excluded column from the stored descriptor but
# not the key referencing it, so determine_compatibility's keys check fails once
# the upstream key is gone. PG/keys analog of MySQL's SS-129. Covers both
# verify_schema call sites: replication (pk_drop) and snapshot (pk_drop_snapshot).
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE pk_drop (id int PRIMARY KEY, val text);
INSERT INTO pk_drop VALUES (1, 'a'), (2, 'b');
# REPLICA IDENTITY FULL so dropping the PK column leaves the keys check as the
# sole reason compatibility could fail.
ALTER TABLE pk_drop REPLICA IDENTITY FULL;

> CREATE TABLE pk_drop
  FROM SOURCE mz_source (REFERENCE public.pk_drop)
  WITH (EXCLUDE COLUMNS (id));

> SELECT * FROM pk_drop;
a
b

# Drop replicas so the second table snapshots against the post-drop schema.
> ALTER CLUSTER storage SET (REPLICATION FACTOR 0)

> CREATE TABLE pk_drop_snapshot
  FROM SOURCE mz_source (REFERENCE public.pk_drop)
  WITH (EXCLUDE COLUMNS (id));

# Drop the excluded PK column upstream. Postgres drops the PK constraint with it.
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pk_drop DROP COLUMN id;

> ALTER CLUSTER storage SET (REPLICATION FACTOR 1)

# Snapshot path: the second table must snapshot against the post-drop schema.
> SELECT * FROM pk_drop_snapshot;
a
b

# Replication path: a write forces a Relation message; both tables must ingest it.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pk_drop VALUES ('c');

> SELECT * FROM pk_drop;
a
b
c

> SELECT * FROM pk_drop_snapshot;
a
b
c
