# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the temporal data types
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

CREATE TABLE t1 (date_col DATE, time_col TIME, timestamp_col TIMESTAMP);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('2011-11-11', '11:11:11.123456', '2011-11-11 11:11:11.123456');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

> CREATE VIEWS FROM SOURCE mz_source;

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(date_col), pg_typeof(time_col), pg_typeof(timestamp_col) FROM t1 LIMIT 1;
date time timestamp

> SELECT * FROM t1;
2011-11-11 11:11:11.123456 "2011-11-11 11:11:11.123456"
2011-11-11 11:11:11.123456 "2011-11-11 11:11:11.123456"
