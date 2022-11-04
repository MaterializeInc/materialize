# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres

DROP PUBLICATION IF EXISTS upgrade_pg_cdc_publication;
CREATE PUBLICATION upgrade_pg_cdc_publication FOR ALL TABLES;

DROP TABLE IF EXISTS cdc_int_table;
CREATE TABLE cdc_int_table (f1 INTEGER);
ALTER TABLE cdc_int_table REPLICA IDENTITY FULL;
INSERT INTO cdc_int_table VALUES (1),(2),(3),(4),(5);

DROP TABLE IF EXISTS cdc_enum_table;
DROP TYPE IF EXISTS an_enum;

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE cdc_enum_table (a an_enum);
INSERT INTO cdc_enum_table VALUES ('var1'), ('var0');
ALTER TABLE cdc_enum_table REPLICA IDENTITY FULL;

DROP TABLE IF EXISTS unused_pub_table;
CREATE TABLE unused_pub_table (f1 INTEGER);
ALTER TABLE unused_pub_table REPLICA IDENTITY FULL;
INSERT INTO unused_pub_table VALUES (1);

# Specifying `TEXT COLUMNS` lets us ingest enum data.
> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres';
> CREATE CONNECTION IF NOT EXISTS pgconn FOR POSTGRES
  HOST postgres,
  USER postgres,
  PASSWORD SECRET pgpass,
  DATABASE postgres;

> CREATE SOURCE upgrade_pg_cdc_source_for_all_tables
  FROM POSTGRES
  CONNECTION pgconn
  (
    PUBLICATION 'upgrade_pg_cdc_publication',
    TEXT COLUMNS (public.an_enum)
  )
  FOR ALL TABLES;

> SELECT * FROM cdc_enum_table;
var1
var0

# If we do not specify a table with an unsupported data type, we do not need to
# specify `TEXT COLUMNS`, even though the table is in the publication. Note that
# this also tests that we do not attempt to produce snapshots of tables in the
# publication that are not referenced in the `FOR TABLES` clause. If we did,
# this would encounter an error during snapshot generation, wherein we could not
# handle the enum data in cdc_enum_table
# https://github.com/MaterializeInc/materialize/issues/15889

> CREATE SOURCE upgrade_pg_cdc_source_for_tables
  FROM POSTGRES
  CONNECTION pgconn
  (
    PUBLICATION 'upgrade_pg_cdc_publication'
  )
  FOR TABLES
  (
    cdc_int_table AS int_table
  );

> SELECT * FROM int_table;
1
2
3
4
5
