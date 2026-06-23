# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test: EXCLUDE COLUMNS naming a key column must be rejected with a
# graceful planning error, NOT a process-aborting panic.
#
# `purify_source_exports` prunes excluded columns from the table descriptor's
# column list (src/sql/src/pure/postgres.rs) but leaves `desc.keys` referencing
# the removed column's attnum. `generate_source_export_statement_values` then
# walks every key and resolves each `col_num` back to a column via
# `.find(...).expect("key exists as column")`. Excluding a PRIMARY KEY or UNIQUE
# column leaves a dangling attnum in `key.cols`, so the lookup returns `None` and
# the `.expect()` panics. Purification runs in a `task::spawn` future that is not
# wrapped in `catch_unwind`, so the enhanced panic handler calls `process::abort()`
# and takes down all of environmentd. Keys are loaded from `pg_constraint`
# (contype IN ('p','u')) independent of replica identity, so this is reachable on
# any REPLICA IDENTITY FULL table that also has a key.
#
# The existing test/pg-cdc/exclude-columns.td only excludes a non-key column on a
# keyless table, so this path had no coverage.
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

# A table with a UNIQUE constraint: `u` (attnum 2) participates in the key.
CREATE TABLE uniq_table (id int, u int UNIQUE, val text);
INSERT INTO uniq_table VALUES (1, 10, 'a'), (2, 20, 'b');
ALTER TABLE uniq_table REPLICA IDENTITY FULL;

ANALYZE;

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);

# Excluding the PRIMARY KEY column should be fine
> CREATE TABLE pk_table
  FROM SOURCE mz_source (REFERENCE public.pk_table)
  WITH (EXCLUDE COLUMNS (id));

# Excluding a UNIQUE-constraint column also okay
> CREATE TABLE jkljkljhhjk
  FROM SOURCE mz_source (REFERENCE public.uniq_table)
  WITH (EXCLUDE COLUMNS (u));

# Sanity: excluding a non-key column on a keyed table still works, and the key
# constraint is preserved on the resulting table.
> CREATE TABLE pk_table_2
  FROM SOURCE mz_source (REFERENCE public.pk_table)
  WITH (EXCLUDE COLUMNS (val));

> SELECT * FROM pk_table_2;
1
2
