# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test postgres EXCLUDE COLUMNS support
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

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE enum_table (a an_enum, b int);
INSERT INTO enum_table VALUES ('var1', 1), ('var0', 0);
ALTER TABLE enum_table REPLICA IDENTITY FULL;

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);

! CREATE TABLE enum_table FROM SOURCE "mz_source" (REFERENCE enum_table);
contains:referenced tables use unsupported types

! CREATE TABLE enum_table
  FROM SOURCE mz_source (REFERENCE public.enum_table)
  WITH (TEXT COLUMNS (a), EXCLUDE COLUMNS (a));
contains:duplicated column name references: ["a"]

> CREATE TABLE enum_table
  FROM SOURCE mz_source (REFERENCE public.enum_table)
  WITH (EXCLUDE COLUMNS (a));

> SELECT * FROM enum_table;
0
1

# Insert more data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO enum_table VALUES ('var1', 3), ('var0', 2);

> SELECT * FROM enum_table;
0
1
2
3

# We now test dropping the excluded column upstream. We want to test that
# Materialize behaves correctly both during snapshotting and during
# replication.

# First we remove all replicas from the cluster so that we can set up the
# objects the way we want.
> ALTER CLUSTER storage SET (REPLICATION FACTOR 0)

# Create a new table from the source with the same excluded tables. This table
# will have to perform its snapshot against the upstream schema with the column
# dropped.
> CREATE TABLE enum_table2
  FROM SOURCE mz_source (REFERENCE public.enum_table)
  WITH (EXCLUDE COLUMNS (a));

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE enum_table DROP COLUMN a;

# Re-add a replica and verify enum_table and enum_table2 have the expected data
> ALTER CLUSTER storage SET (REPLICATION FACTOR 1)

> SELECT * FROM enum_table;
0
1
2
3

> SELECT * FROM enum_table2;
0
1
2
3

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO enum_table VALUES (4);

# Verify replication ingestion works as expected
> SELECT * FROM enum_table;
0
1
2
3
4

> SELECT * FROM enum_table2;
0
1
2
3
4
