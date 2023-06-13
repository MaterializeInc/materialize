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

DROP TABLE IF EXISTS unused_pub_table;
CREATE TABLE unused_pub_table (f1 INTEGER);
ALTER TABLE unused_pub_table REPLICA IDENTITY FULL;
INSERT INTO unused_pub_table VALUES (1);

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

DROP TABLE IF EXISTS cdc_empty_on_boot_table;
CREATE TABLE cdc_empty_on_boot_table (a int);
ALTER TABLE cdc_empty_on_boot_table REPLICA IDENTITY FULL;

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
    TEXT COLUMNS (cdc_enum_table.a)
  )
  FOR ALL TABLES;

> SELECT * FROM cdc_enum_table;
var1
var0

> SELECT * FROM cdc_empty_on_boot_table;

# If we do not specify a table with an unsupported data type, we do not need to
# specify `TEXT COLUMNS`, even though the table is in the publication.

> CREATE SOURCE upgrade_pg_cdc_source_for_tables
  FROM POSTGRES
  CONNECTION pgconn
  (
    PUBLICATION 'upgrade_pg_cdc_publication'
  )
  FOR TABLES
  (
    cdc_int_table AS cdc_int_table_for_tables
  );

> SELECT * FROM cdc_int_table_for_tables;
1
2
3
4
5

$ skip-if
SELECT mz_version_num() >= 5600;

# check that we support creating a source that supports referencing the same upstream table from
# multiple subsources; the test is simply that this doesn't fail to upgrade to later # versions of
# Materialize where this behavior is prohibited.

> CREATE SOURCE upgrade_pg_cdc_duplicate_references
  FROM POSTGRES
  CONNECTION pgconn
  (
    PUBLICATION 'upgrade_pg_cdc_publication'
  )
  FOR TABLES
  (
    cdc_int_table AS cdc_int_table_zero,
    cdc_int_table AS cdc_int_table_one
  );

> SELECT * FROM cdc_int_table_zero;
1
2
3
4
5

> SELECT * FROM cdc_int_table_one;
1
2
3
4
5
