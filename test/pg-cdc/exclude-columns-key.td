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
CREATE TABLE uniq_table (id int, u int, val text, UNIQUE (id, u));
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
> CREATE TABLE uniq_table
  FROM SOURCE mz_source (REFERENCE public.uniq_table)
  WITH (EXCLUDE COLUMNS (u));

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

# Sanity: excluding a non-key column on a keyed table still works, and the key
# constraint is preserved on the resulting table.
> CREATE TABLE pk_table_2
  FROM SOURCE mz_source (REFERENCE public.pk_table)
  WITH (EXCLUDE COLUMNS (val));

> SELECT * FROM pk_table_2;
1
2
