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
