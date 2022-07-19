# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the temporal data types with time zone
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE time_table (f1 TIME WITH TIME ZONE);
INSERT INTO time_table VALUES ('11:11:11.123456-09');
INSERT INTO time_table VALUES ('11:11:11.123456+09');
ALTER TABLE time_table REPLICA IDENTITY FULL;

CREATE TABLE timestamp_table (f1 TIMESTAMP WITH TIME ZONE);
INSERT INTO timestamp_table VALUES ('2011-11-11 11:11:11.123456-09');
INSERT INTO timestamp_table VALUES ('2011-11-11 11:11:11.123456+09');
ALTER TABLE timestamp_table REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source (timestamp_table);

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO time_table SELECT * FROM time_table;
INSERT INTO timestamp_table SELECT * FROM timestamp_table;

> SELECT pg_typeof(f1) FROM timestamp_table LIMIT 1;
"timestamp with time zone"

> SELECT * FROM timestamp_table;
"2011-11-11 02:11:11.123456 UTC"
"2011-11-11 02:11:11.123456 UTC"
"2011-11-11 20:11:11.123456 UTC"
"2011-11-11 20:11:11.123456 UTC"

# Mz does not support TIME WITH TIME ZOINE
! CREATE VIEWS FROM SOURCE mz_source (time_table);
contains:unknown catalog item 'pg_catalog.timetz'
