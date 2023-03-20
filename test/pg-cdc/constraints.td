# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

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

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

INSERT INTO pk_table VALUES (3, 'three');

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER);
INSERT INTO nonpk_table VALUES (1, 1), (1, 1);
ALTER TABLE nonpk_table REPLICA IDENTITY FULL;
INSERT INTO nonpk_table VALUES (2, 2), (2, 2);

CREATE TABLE complex_constraints (pk INTEGER, f2 TEXT NOT NULL, f3 TEXT, UNIQUE (f3, pk), UNIQUE (f2));
ALTER TABLE complex_constraints ADD PRIMARY KEY (pk);
INSERT INTO complex_constraints VALUES (1, 'one', 1);
ALTER TABLE complex_constraints REPLICA IDENTITY FULL;
INSERT INTO complex_constraints VALUES (2, 'two', 2);
INSERT INTO complex_constraints VALUES (3, 'three', 3);

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR TABLES (pk_table, nonpk_table, complex_constraints);

> CREATE DEFAULT INDEX ON pk_table;

# Default index only includes primary key column
> SHOW CREATE INDEX pk_table_primary_idx;
name                                     create_sql
-------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.pk_table_primary_idx "CREATE INDEX \"pk_table_primary_idx\" IN CLUSTER \"default\" ON \"materialize\".\"public\".\"pk_table\" (\"pk\")"

# Not null constraint optimizes like this:
> EXPLAIN SELECT * FROM pk_table WHERE pk IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n"

# Unique is applied
> EXPLAIN SELECT DISTINCT pk FROM pk_table
"Explained Query (fast path):\n  Project (#0)\n    ReadExistingIndex materialize.public.pk_table_primary_idx\n\nUsed Indexes:\n  - materialize.public.pk_table_primary_idx\n"

> CREATE DEFAULT INDEX ON nonpk_table;

# Default index only includes primary key column
> SHOW CREATE INDEX nonpk_table_primary_idx;
name                                     create_sql
-------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.nonpk_table_primary_idx "CREATE INDEX \"nonpk_table_primary_idx\" IN CLUSTER \"default\" ON \"materialize\".\"public\".\"nonpk_table\" (\"f1\", \"f2\")"

# No not-null constraint
> EXPLAIN SELECT * FROM nonpk_table WHERE f1 IS NULL;
"Explained Query (fast path):\n  Filter (#0) IS NULL\n    ReadExistingIndex materialize.public.nonpk_table_primary_idx\n\nUsed Indexes:\n  - materialize.public.nonpk_table_primary_idx\n"

> CREATE DEFAULT INDEX ON complex_constraints;

# Default index only includes primary key column
> SHOW CREATE INDEX complex_constraints_primary_idx;
name                                     create_sql
-------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.complex_constraints_primary_idx "CREATE INDEX \"complex_constraints_primary_idx\" IN CLUSTER \"default\" ON \"materialize\".\"public\".\"complex_constraints\" (\"pk\")"

# Not null constraint optimizes like this:
> EXPLAIN SELECT * FROM pk_table WHERE pk IS NULL;
"Explained Query (fast path):\n  Constant <empty>\n"
